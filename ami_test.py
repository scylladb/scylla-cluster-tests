#!/usr/bin/env python

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation; either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright (c) 2016 ScyllaDB


from avocado import main

from ami import AMIBaseTest

from sdcm.tester import ClusterTester

class MicroAMITest(AMIBaseTest, ClusterTester):

    """
    :avocado: enable
    """

    INSTANCE_TYPE = "t2.micro"

    def test_ami(self):
        super(MicroAMITest, self).test_ami()

class C3LargeAMITest(AMIBaseTest):

    """
    :avocado: enable
    """

    INSTANCE_TYPE = "c3.large"

    def test_ami(self):
        super(C3LargeAMITest, self).test_ami()

class C3XLargeAMITest(AMIBaseTest):

    """
    :avocado: enable
    """

    INSTANCE_TYPE = "c3.xlarge"

    def test_ami(self):
        super(C3XLargeAMITest, self).test_ami()

class C32xLargeAMITest(AMIBaseTest):

    """
    :avocado: enable
    """

    INSTANCE_TYPE = "c3.2xlarge"

    def test_ami(self):
        super(C32xLargeAMITest, self).test_ami()

class C34xLargeAMITest(AMIBaseTest):

    """
    :avocado: enable
    """

    INSTANCE_TYPE = "c3.4xlarge"

    def test_ami(self):
        super(C34xLargeAMITest, self).test_ami()

class C38xLargeAMITest(AMIBaseTest):

    """
    :avocado: enable
    """

    INSTANCE_TYPE = "c3.8xlarge"

    def test_ami(self):
        super(C38xLargeAMITest, self).test_ami()

class M3MediumAMITest(AMIBaseTest):

    """
    :avocado: enable
    """

    INSTANCE_TYPE = "m3.medium"

    def test_ami(self):
        super(M3MediumAMITest, self).test_ami()

class M3LargeAMITest(AMIBaseTest):

    """
    :avocado: enable
    """

    INSTANCE_TYPE = "m3.large"

    def test_ami(self):
        super(M3LargeAMITest, self).test_ami()

class M3XLargeAMITest(AMIBaseTest):

    """
    :avocado: enable
    """

    INSTANCE_TYPE = "m3.xlarge"

    def test_ami(self):
        super(M3XLargeAMITest, self).test_ami()

class M32XLargeMediumAMITest(AMIBaseTest):

    """
    :avocado: enable
    """

    INSTANCE_TYPE = "m3.2xlarge"

    def test_ami(self):
        super(M32XLargeMediumAMITest, self).test_ami()

class I2XlargeAMITest(AMIBaseTest):

    """
    :avocado: enable
    """

    INSTANCE_TYPE = "i2.xlarge"

    def test_ami(self):
        super(I2XlargeAMITest, self).test_ami()

class I22XlargeAMITest(AMIBaseTest):

    """
    :avocado: enable
    """

    INSTANCE_TYPE = "i2.2xlarge"

    def test_ami(self):
        super(I22XlargeAMITest, self).test_ami()

class I24XlargeAMITest(AMIBaseTest):

    """
    :avocado: enable
    """

    INSTANCE_TYPE = "i2.4xlarge"

    def test_ami(self):
        super(I24XlargeAMITest, self).test_ami()

class I28XlargeAMITest(AMIBaseTest):

    """
    :avocado: enable
    """

    INSTANCE_TYPE = "i2.8xlarge"

    def test_ami(self):
        super(I28XlargeAMITest, self).test_ami()

if __name__ == '__main__':
    main()
