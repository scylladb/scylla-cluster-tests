import logging
import datetime
import time
import boto3
import base64
from botocore.exceptions import ClientError, NoRegionError

from .utils import retrying

logger = logging.getLogger(__name__)

STATUS_FULFILLED = 'fulfilled'
STATUS_PRICE_TOO_LOW = 'price-too-low'
STATUS_ERROR = 'error'
FLEET_LIMIT_EXCEEDED_ERROR = 'spotInstanceCountLimitExceeded'
MAX_SPOT_EXCEEDED_ERROR = 'MaxSpotInstanceCountExceeded'
REQUEST_TIMEOUT = 300


class GetSpotPriceHistoryError(Exception):
    pass


class CreateSpotInstancesError(Exception):
    pass


class GetInstanceByPrivateIpError(Exception):
    pass


class CreateEC2ClientNoRegionError(NoRegionError):
    pass


class CreateSpotFleetError(ClientError):
    pass


class EC2Client(object):

    def __init__(self, timeout=REQUEST_TIMEOUT, region_name=None):
        self._client = self._get_ec2_client(region_name)
        self._resource = boto3.resource('ec2')
        self._timeout = timeout  # request timeout in seconds
        self._price_index = 1.5
        self._wait_interval = 5  # seconds

    def _get_ec2_client(self, region_name=None):
            try:
                return boto3.client('ec2')
            except NoRegionError:
                if not region_name:
                    raise CreateEC2ClientNoRegionError()
                # next class instance could be created without region_name parameter
                boto3.setup_default_session(region_name=region_name)
                return self._get_ec2_client()

    def _request_spot_instance(self, instance_type, image_id, region_name, network_if, spot_price, key_pair='',
                               user_data='', count=1, duration=0, request_type='one-time', block_device_mappings=None):
        """
        Create a spot instance request
        :return: list of request id-s
        """
        params = dict(DryRun=False,
                      InstanceCount=count,
                      Type=request_type,
                      SpotPrice=str(spot_price),
                      LaunchSpecification={'ImageId': image_id,
                                           'InstanceType': instance_type,
                                           'NetworkInterfaces': network_if,
                                           },
                      ValidUntil=datetime.datetime.now() + datetime.timedelta(minutes=self._timeout/60 + 5)
                      )
        logger.debug("block_device_mappings: %s" % block_device_mappings)
        if block_device_mappings:
            params['LaunchSpecification']['BlockDeviceMappings'] = block_device_mappings
        if not duration:
            params.update({'AvailabilityZoneGroup': region_name})
        else:
            params.update({'BlockDurationMinutes': int(duration)})
        if key_pair:
            params['LaunchSpecification'].update({'KeyName': key_pair})
        if user_data:
            params['LaunchSpecification'].update({'UserData': self._encode_user_data(user_data)})

        logger.info('Sending spot request with params: %s', params)
        resp = self._client.request_spot_instances(**params)

        request_ids = [req['SpotInstanceRequestId'] for req in resp['SpotInstanceRequests']]
        logger.debug('Spot requests: %s', request_ids)
        return request_ids

    def _request_spot_fleet(self, instance_type, image_id, region_name, network_if, key_pair='', user_data='', count=3,
                            block_device_mappings=None):
        spot_price = self._get_spot_price(instance_type)
        fleet_config = {'LaunchSpecifications':
                        [
                            {'ImageId': image_id,
                             'InstanceType': instance_type,
                             'NetworkInterfaces': network_if,
                             'Placement': {'AvailabilityZone': region_name},
                             },
                        ],
                        'IamFleetRole': 'arn:aws:iam::797456418907:role/aws-ec2-spot-fleet-role',
                        'SpotPrice': str(spot_price['desired']),
                        'TargetCapacity': count,
                        }
        if key_pair:
            fleet_config['LaunchSpecifications'][0].update({'KeyName': key_pair})
        if user_data:
            fleet_config['LaunchSpecifications'][0].update({'UserData': self._encode_user_data(user_data)})
        if block_device_mappings:
            fleet_config['LaunchSpecifications'][0]['BlockDeviceMappings'] = block_device_mappings
        logger.info('Sending spot fleet request with params: %s', fleet_config)
        resp = self._client.request_spot_fleet(DryRun=False,
                                               SpotFleetRequestConfig=fleet_config)

        request_id = resp['SpotFleetRequestId']
        logger.debug('Spot fleet request: %s', request_id)
        return request_id

    def _get_spot_price(self, instance_type, region_name=''):
        """
        Calculate spot price for bidding
        :return: spot bid price
        """
        logger.info('Calculating spot price for bidding')
        history = self._client.describe_spot_price_history(InstanceTypes=[instance_type], AvailabilityZone=region_name)
        if 'SpotPriceHistory' not in history or not history['SpotPriceHistory']:
            raise GetSpotPriceHistoryError("Failed getting spot price history for instance type %s", instance_type)
        prices = [float(item['SpotPrice']) for item in history['SpotPriceHistory']]
        price_avg = round(sum(prices) / len(prices), 4)
        price_min = min(prices)
        price_desired = (price_avg + price_min) / 4
        if price_desired < price_min:
            price_desired = price_min
        price = (dict(min=price_min, max=max(prices), avg=price_avg, desired=round(price_desired, 4)))
        logger.info('Spot bid price: %s', price)
        return price

    def _is_request_fulfilled(self, request_ids):
        """
        Check request status
        """
        resp = self._client.describe_spot_instance_requests(SpotInstanceRequestIds=request_ids)
        for req in resp['SpotInstanceRequests']:
            if req['Status']['Code'] != STATUS_FULFILLED or req['State'] != 'active':
                if req['Status']['Code'] == STATUS_PRICE_TOO_LOW:
                    return False, req['Status']['Code']
                return False, resp
        return True, resp

    def _wait_for_request_done(self, request_ids):
        """
        Wait for spot requests fulfilled
        :param request_ids: spot request id-s
        :return: list of spot instance id-s
        """
        logger.info('Waiting for spot instances...')
        timeout = 0
        status = False
        while not status and timeout < self._timeout:
            time.sleep(self._wait_interval)
            status, resp = self._is_request_fulfilled(request_ids)
            if not status and resp == STATUS_PRICE_TOO_LOW:
                break
            timeout += self._wait_interval
        if not status:
            self._client.cancel_spot_instance_requests(SpotInstanceRequestIds=request_ids)
            return [], resp
        return [req['InstanceId'] for req in resp['SpotInstanceRequests']], resp

    def _is_fleet_request_fulfilled(self, request_id):
        """
        Check fleet request status
        """
        resp = self._client.describe_spot_fleet_requests(SpotFleetRequestIds=[request_id])
        for req in resp['SpotFleetRequestConfigs']:
            if req['SpotFleetRequestState'] != 'active' or 'ActivityStatus' not in req or\
                            req['ActivityStatus'] != STATUS_FULFILLED:
                if 'ActivityStatus' in req and req['ActivityStatus'] == STATUS_ERROR:
                    tt = datetime.datetime.now().timetuple()
                    search_start_time = datetime.datetime(tt.tm_year, tt.tm_mon, tt.tm_mday)
                    resp = self._client.describe_spot_fleet_request_history(SpotFleetRequestId=request_id,
                                                                            StartTime=search_start_time,
                                                                            MaxResults=10)
                    logger.debug('Fleet request error history: %s', resp)
                    errors = [i['EventInformation']['EventSubType'] for i in resp['HistoryRecords']]
                    if FLEET_LIMIT_EXCEEDED_ERROR in errors:
                        return False, FLEET_LIMIT_EXCEEDED_ERROR
                return False, resp
        return True, resp

    def _wait_for_fleet_request_done(self, request_id):
        """
        Wait for spot fleet request fulfilled
        :param request_id: spot fleet request id
        :return: list of spot instance id-s
        """
        logger.info('Waiting for spot fleet...')
        timeout = 0
        status = False
        while not status and timeout < self._timeout:
            time.sleep(self._wait_interval)
            status, resp = self._is_fleet_request_fulfilled(request_id)
            if not status and resp == FLEET_LIMIT_EXCEEDED_ERROR:
                break
            timeout += self._wait_interval
        if not status:
            self._client.cancel_spot_fleet_requests(SpotFleetRequestIds=[request_id], TerminateInstances=True)
            return [], resp
        resp = self._client.describe_spot_fleet_instances(SpotFleetRequestId=request_id)
        return [inst['InstanceId'] for inst in resp['ActiveInstances']], resp

    def _encode_user_data(self, user_data):
        return base64.b64encode(user_data)

    def get_instance(self, instance_id):
        """
        Get instance object by id
        :param instance_id: instance id
        :return: EC2.Instance object
        """
        instance = self._resource.Instance(id=instance_id)
        return instance

    @retrying(n=5, sleep_time=10, allowed_exceptions=(ClientError,),
              message="Waiting for instance is available")
    def add_tags(self, instance_id, tags=[]):
        """
        Add tags to instance
        """
        self._client.create_tags(Resources=[instance_id], Tags=tags)

    def create_spot_instances(self, instance_type, image_id, region_name, network_if, key_pair='', user_data='',
                              count=1, duration=0, block_device_mappings=None):
        """
        Create spot instances

        :param instance_type: instance type
        :param image_id: image id
        :param region_name: availability zone name
        :param network_if: network interfaces
        :param key_pair: user credentials
        :param user_data: user data to be passed to instances
        :param count: number of instances to launch
        :param duration(optional): instance life time in minutes(multiple of 60)
        :return: list of instance id-s
        """
        instance_ids = []
        spot_price = self._get_spot_price(instance_type)
        price_desired = spot_price['desired']
        while not instance_ids and price_desired <= spot_price['avg']:
            request_ids = self._request_spot_instance(instance_type, image_id, region_name, network_if, price_desired,
                                                      key_pair, user_data, count, duration,
                                                      block_device_mappings=block_device_mappings)
            instance_ids, resp = self._wait_for_request_done(request_ids)
            if not instance_ids and resp == STATUS_PRICE_TOO_LOW:
                price_desired = round(price_desired * self._price_index, 4)
                if price_desired <= spot_price['avg']:
                    logger.info('Got price-too-low, retrying with higher price %s', price_desired)
                    continue
        if not instance_ids:
            raise CreateSpotInstancesError("Failed to get spot instances: %s" % resp)

        logger.info('Spot instances: %s', instance_ids)
        for ind, instance_id in enumerate(instance_ids):
            self.add_tags(instance_id, [{'Key': 'Name', 'Value': 'spot_{}_{}'.format(instance_id, ind)}])

        self._client.cancel_spot_instance_requests(SpotInstanceRequestIds=request_ids)

        instances = [self.get_instance(instance_id) for instance_id in instance_ids]
        return instances

    def create_spot_fleet(self, instance_type, image_id, region_name, network_if, key_pair='', user_data='', count=3,
                          block_device_mappings=None):
        """
        Create spot fleet
        :param instance_type: instance type
        :param image_id: image id
        :param region_name: availability zone name
        :param network_if: network interfaces
        :param key_pair: user credentials
        :param user_data: user data to be passed to instances
        :param count: number of instances to launch
        :return: list of instance id-s
        """
        request_id = self._request_spot_fleet(instance_type, image_id, region_name, network_if, key_pair,
                                              user_data, count, block_device_mappings=block_device_mappings)
        instance_ids, resp = self._wait_for_fleet_request_done(request_id)
        if not instance_ids:
            err_code = MAX_SPOT_EXCEEDED_ERROR if resp == FLEET_LIMIT_EXCEEDED_ERROR else STATUS_ERROR
            raise CreateSpotFleetError(error_response={'Error': {'Code': err_code, 'Message': resp}},
                                       operation_name='create_spot_fleet')

        logger.info('Spot instances: %s', instance_ids)
        for ind, instance_id in enumerate(instance_ids):
            self.add_tags(instance_id, [{'Key': 'Name', 'Value': 'spot_fleet_{}_{}'.format(instance_id, ind)}])

        self._client.cancel_spot_fleet_requests(SpotFleetRequestIds=[request_id], TerminateInstances=False)

        instances = [self.get_instance(instance_id) for instance_id in instance_ids]
        return instances

    def terminate_instances(self, instance_ids):
        """
        Terminate instances
        """
        self._client.terminate_instances(InstanceIds=instance_ids)

    def get_subnet_info(self, subnet_id):
        resp = self._client.describe_subnets(SubnetIds=[subnet_id])
        return [subnet for subnet in resp['Subnets'] if subnet['SubnetId'] == subnet_id][0]

    def get_instance_by_private_ip(self, private_ip):
        """
        Find instance by private IP address
        :param private_ip: private IP address
        :return: EC2.Instance object
        """
        instances = self._client.describe_instances(Filters=[
            {'Name': 'private-ip-address',
             'Values': [private_ip]}
        ])
        if not instances['Reservations']:
            raise GetInstanceByPrivateIpError("Cannot find instance by private ip: %s" % private_ip)

        return self.get_instance(instances['Reservations'][0]['Instances'][0]['InstanceId'])


if __name__ == '__main__':
    logging.basicConfig(filename='/tmp/ec2_client.log',
                        filemode='w',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.INFO)

    network_if = [{'DeviceIndex': 0,
                   'SubnetId': 'subnet-ad3ce9f4',
                   'AssociatePublicIpAddress': True,
                   'Groups': ['sg-5e79983a']}]
    user_data = '--clustername cluster-scale-test-xxx --bootstrap true --totalnodes 3'

    ec2 = EC2Client(region_name='us-east1')
    instance_type = 'm3.medium'
    image_id = 'ami-56373b2d'
    avail_zone = ec2.get_subnet_info(network_if[0]['SubnetId'])['AvailabilityZone']

    inst = ec2.create_spot_instances(instance_type, image_id, avail_zone, network_if, user_data=user_data, count=2)
    print inst
    inst = ec2.create_spot_instances(instance_type, image_id, avail_zone, network_if, count=1, duration=60)
    print inst
    inst = ec2.create_spot_fleet(instance_type, image_id, avail_zone, network_if, user_data=user_data, count=2)
    print inst
