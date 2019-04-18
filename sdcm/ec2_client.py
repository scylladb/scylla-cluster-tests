import logging
import datetime
import time
import json
import base64

import boto3
from botocore.exceptions import ClientError, NoRegionError

from sdcm.utils.common import retrying


LOGGER = logging.getLogger(__name__)

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

    def __init__(self, timeout=REQUEST_TIMEOUT, region_name=None, spot_max_price_percentage=None):
        self._client = self._get_ec2_client(region_name)
        self._resource = boto3.resource('ec2', region_name=region_name)
        self.region_name = region_name
        self._timeout = timeout  # request timeout in seconds
        self._price_index = 1.5
        self._wait_interval = 5  # seconds
        self.spot_max_price_percentage = spot_max_price_percentage

    def _get_ec2_client(self, region_name=None):
        try:
            return boto3.client(service_name='ec2', region_name=region_name)
        except NoRegionError:
            if not region_name:
                raise CreateEC2ClientNoRegionError()
            # next class instance could be created without region_name parameter
            boto3.setup_default_session(region_name=region_name)
            return self._get_ec2_client()

    def _request_spot_instance(self, instance_type, image_id, region_name, network_if, spot_price, key_pair='',  # pylint: disable=too-many-arguments
                               user_data='', count=1, duration=0, request_type='one-time', block_device_mappings=None):
        """
        Create a spot instance request
        :return: list of request id-s
        """

        # pylint: disable=too-many-locals
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
        LOGGER.debug("block_device_mappings: %s", block_device_mappings)
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

        LOGGER.info('Sending spot request with params: %s', params)
        resp = self._client.request_spot_instances(**params)

        request_ids = [req['SpotInstanceRequestId'] for req in resp['SpotInstanceRequests']]
        LOGGER.debug('Spot requests: %s', request_ids)
        return request_ids

    def _request_spot_fleet(self, instance_type, image_id, region_name, network_if, key_pair='', user_data='', count=3,  # pylint: disable=too-many-arguments
                            block_device_mappings=None, tags_list=None):

        tags_list = tags_list if tags_list else []
        spot_price = self._get_spot_price(instance_type)
        fleet_config = {'LaunchSpecifications':
                        [
                            {'ImageId': image_id,
                             'InstanceType': instance_type,
                             'NetworkInterfaces': network_if,
                             'Placement': {'AvailabilityZone': region_name},
                             'TagSpecifications': [
                                 {
                                     'ResourceType': 'instance',
                                     'Tags': tags_list
                                 }

                             ]
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
        LOGGER.info('Sending spot fleet request with params: %s', fleet_config)
        resp = self._client.request_spot_fleet(DryRun=False,
                                               SpotFleetRequestConfig=fleet_config)

        request_id = resp['SpotFleetRequestId']
        LOGGER.debug('Spot fleet request: %s', request_id)
        return request_id

    @staticmethod
    def get_instance_price(region_name, instance_type):
        regions_names_map = {
            'us-east-2': 'US East (Ohio)',
            'us-east-1': 'US East (N. Virginia)',
            'us-west-1': 'US West (N. California)',
            'us-west-2': 'US West (Oregon)',
            'ap-south-1': 'Asia Pacific (Mumbai)',
            'ap-northeast-3': 'Asia Pacific (Osaka-Local)',
            'ap-northeast-2': 'Asia Pacific (Seoul)',
            'ap-southeast-1': 'Asia Pacific (Singapore)',
            'ap-southeast-2': 'Asia Pacific (Sydney)',
            'ap-northeast-1': 'Asia Pacific (Tokyo)',
            'ca-central-1': 'Canada (Central)',
            'eu-central-1': 'EU (Frankfurt)',
            'eu-west-1': 'EU (Ireland)',
            'eu-west-2': 'EU (London)',
            'eu-west-3': 'EU (Paris)',
            'eu-north-1': 'EU (Stockholm)',
            'sa-east-1': 'South America (Sao Paulo)'
        }

        pricing = boto3.client('pricing', region_name='us-east-1')
        response = pricing.get_products(
            ServiceCode='AmazonEC2',
            Filters=[
                {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': 'Linux'},
                {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_type},
                {'Type': 'TERM_MATCH', 'Field': 'preInstalledSw', 'Value': 'NA'},
                {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': 'Shared'},
                {'Type': 'TERM_MATCH', 'Field': 'capacitystatus', 'Value': 'Used'},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': regions_names_map[region_name]}
            ],
            MaxResults=10
        )
        assert response['PriceList'], "failed to get price for {instance_type} in {region_name}".format(
            region_name=region_name, instance_type=instance_type)
        price = response['PriceList'][0]
        price_dimensions = next(iter(json.loads(price)['terms']['OnDemand'].values()))['priceDimensions']
        instance_price = next(iter(price_dimensions.values()))['pricePerUnit']['USD']
        return instance_price

    def _get_spot_price(self, instance_type):
        """
        Calculate spot price for bidding
        :return: spot bid price
        """
        LOGGER.info('Calculating spot price based on OnDemand price')

        on_demand_price = float(self.get_instance_price(self.region_name, instance_type))

        price = dict(max=on_demand_price, desired=on_demand_price * self.spot_max_price_percentage)
        LOGGER.info('Spot bid price: %s', price)
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
        LOGGER.info('Waiting for spot instances...')
        timeout = 0
        status = False
        while not status and timeout < self._timeout:
            time.sleep(self._wait_interval)
            status, resp = self._is_request_fulfilled(request_ids)
            LOGGER.debug("{request_ids}: [{status}] - {resp}".format(**locals()))
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
                    current_time = datetime.datetime.now().timetuple()
                    search_start_time = datetime.datetime(
                        current_time.tm_year, current_time.tm_mon, current_time.tm_mday)
                    resp = self._client.describe_spot_fleet_request_history(SpotFleetRequestId=request_id,
                                                                            StartTime=search_start_time,
                                                                            MaxResults=10)
                    LOGGER.debug('Fleet request error history: %s', resp)
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
        LOGGER.info('Waiting for spot fleet...')
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

    @staticmethod
    def _encode_user_data(user_data):
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
    def add_tags(self, instance_id, tags=None):
        """
        Add tags to instance
        """
        tags = tags if tags else []
        self._client.create_tags(Resources=[instance_id], Tags=tags)

    def create_spot_instances(self, instance_type, image_id, region_name, network_if, key_pair='', user_data='',  # pylint: disable=too-many-arguments
                              count=1, duration=0, block_device_mappings=None, tags_list=None):
        """
        Create spot instances

        :param instance_type: instance type
        :param image_id: image id
        :param region_name: availability zone name
        :param network_if: network interfaces
        :param key_pair: user credentials
        :param user_data: user data to be passed to instances
        :param count: number of instances to launch
        :param duration: (optional) instance life time in minutes(multiple of 60)
        :param tags_list: list of tags to assign to fleet instances

        :return: list of instance id-s
        """

        # pylint: disable=too-many-locals

        tags_list = tags_list if tags_list else []
        spot_price = self._get_spot_price(instance_type)

        request_ids = self._request_spot_instance(instance_type, image_id, region_name, network_if, spot_price['desired'],
                                                  key_pair, user_data, count, duration,
                                                  block_device_mappings=block_device_mappings)
        instance_ids, resp = self._wait_for_request_done(request_ids)

        if not instance_ids:
            raise CreateSpotInstancesError("Failed to get spot instances: %s" % resp)

        LOGGER.info('Spot instances: %s', instance_ids)
        for ind, instance_id in enumerate(instance_ids):
            self.add_tags(instance_id, [{'Key': 'Name', 'Value': 'spot_{}_{}'.format(instance_id, ind)}])

        self._client.cancel_spot_instance_requests(SpotInstanceRequestIds=request_ids)

        instances = [self.get_instance(instance_id) for instance_id in instance_ids]
        return instances

    def create_spot_fleet(self, instance_type, image_id, region_name, network_if, key_pair='', user_data='', count=3,  # pylint: disable=too-many-arguments
                          block_device_mappings=None, tags_list=None):
        """
        Create spot fleet
        :param instance_type: instance type
        :param image_id: image id
        :param region_name: availability zone name
        :param network_if: network interfaces
        :param key_pair: user credentials
        :param user_data: user data to be passed to instances
        :param count: number of instances to launch
        :param block_device_mappings:
        :param tags_list: list of tags to assign to fleet instances
        :return: list of instance id-s
        """
        # pylint: disable=too-many-locals

        tags_list = tags_list if tags_list else []

        request_id = self._request_spot_fleet(instance_type, image_id, region_name, network_if, key_pair,
                                              user_data, count, block_device_mappings=block_device_mappings, tags_list=tags_list)
        instance_ids, resp = self._wait_for_fleet_request_done(request_id)
        if not instance_ids:
            err_code = MAX_SPOT_EXCEEDED_ERROR if resp == FLEET_LIMIT_EXCEEDED_ERROR else STATUS_ERROR
            raise CreateSpotFleetError(error_response={'Error': {'Code': err_code, 'Message': resp}},
                                       operation_name='create_spot_fleet')

        LOGGER.info('Spot instances: %s', instance_ids)
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
