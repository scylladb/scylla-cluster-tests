import logging
import datetime
import time
import boto3

logger = logging.getLogger(__name__)


class EC2Client(object):

    def __init__(self, params, timeout=15):
        self._client = boto3.client('ec2')
        self._timeout = timeout  # minutes
        self._params = params

    def _request_spot_instance(self, instance_type, image_id, region_name, count=1, request_type='one-time'):
        spot_price = self._get_spot_price(instance_type)
        logger.info('Sending spot request')
        network_if = [
                        {'DeviceIndex': 0,
                         'SubnetId': self._params.get('subnet_id'),
                         'AssociatePublicIpAddress': True,
                         'Groups': self._params.get('security_group_ids').split()}
                    ]
        resp = self._client.request_spot_instances(DryRun=False,
                                                   SpotPrice=spot_price,
                                                   InstanceCount=count,
                                                   Type=request_type,
                                                   LaunchSpecification={'ImageId': image_id,
                                                                        'InstanceType': instance_type,
                                                                        'NetworkInterfaces': network_if},
                                                   AvailabilityZoneGroup=region_name,
                                                   ValidUntil=datetime.datetime.now() + datetime.timedelta(
                                                       minutes=self._timeout))

        request_ids = [req['SpotInstanceRequestId'] for req in resp['SpotInstanceRequests']]
        logger.debug('Spot requests: %s', request_ids)
        return request_ids

    def _request_spot_fleet(self):
        pass

    def _get_spot_price(self, instance_type, region_name=''):
        logger.info('Calculating spot price for bidding')
        history = self._client.describe_spot_price_history(InstanceTypes=[instance_type], AvailabilityZone=region_name)
        prices = [float(item['SpotPrice']) for item in history['SpotPriceHistory']]
        price = (sum(prices) / len(prices) + min(prices)) / 4
        price = str(round(price, 4))
        logger.info('Spot bid price: %s', price)
        return price

    def _is_request_fulfilled(self, request_ids):
        resp = self._client.describe_spot_instance_requests(SpotInstanceRequestIds=request_ids)
        for req in resp['SpotInstanceRequests']:
            if req['Status']['Code'] != 'fulfilled' or req['State'] != 'active':
                return False, resp
        return True, resp

    def _wait_for_request_done(self, request_ids):
        logger.info('Waiting for spot instances...')
        timeout = 0
        status = False
        while not status and timeout < self._timeout:
            status, resp = self._is_request_fulfilled(request_ids)
            time.sleep(1)
            timeout += 1
        if not status:
            self._client.cancel_spot_instance_requests(SpotInstanceRequestIds=request_ids)
            raise "Failed to get spot instances: %s" % resp
        return [req['InstanceId'] for req in resp['SpotInstanceRequests']]

    def add_tags(self, instance_id, tags=[]):
        self._client.create_tags(Resources=[instance_id], Tags=tags)

    def create_spot_instances(self, instance_type, image_id, region_name, count=1):
        request_ids = self._request_spot_instance(instance_type, image_id, region_name, count)
        instance_ids = self._wait_for_request_done(request_ids)
        logger.info('Spot instances: %s', instance_ids)
        for ind, instance_id in enumerate(instance_ids):
            self.add_tags(instance_id, [{'Key': 'Name', 'Value': 'spot_{}_{}'.format(instance_id, ind)}])
        return instance_ids

    def terminate_instances(self, instance_ids):
        self._client.terminate_instances(InstanceIds=instance_ids)


if __name__ == '__main__':
    logging.basicConfig(filename='/tmp/ec2_client.log',
                        filemode='w',
                        format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                        datefmt='%H:%M:%S',
                        level=logging.INFO)

    params = {'subnet_id': 'subnet-d934e980',
              'security_group_ids': 'sg-c5e1f7a0'}

    ec2 = EC2Client(params)
    ids = ec2.create_spot_instances('m3.medium', 'ami-b390ccc8', 'us-east-1c', count=2)
    print ids
