#!/usr/bin/env python

"""
Tests for Express ESX Host
"""

purpose = """Verify ESX host API"""

import sys

sys.path.append('../..')
sys.path.append('../../express')

# Importing express
import express
import utils
import uuid
from frtestcase import *
from frargs import ARGS
from frlog import LOG
from frexceptions import *


ARGS.parser.add_argument(
    "--esxhost", type=str,
    default='',
    help="ESXi host name.")

ARGS.parser.add_argument(
    "--sourcehost", type=str,
    default='mls.marslab.netapp.com',
    help="Name of remote host with mounted NFS share containing VM templates. Default: 'mls.marslab.netapp.com'")

ARGS.parser.add_argument(
    "--sourcepath", type=str,
    default='lab/template_vm',
    help="Path on NFS share to folder with VM templates. Default: 'lab/template/vm'")

ARGS.parser.add_argument(
    "--cleanupmars", type=bool,
    default=True,
    help="Cleanup Mars as part of test Teardown. Default: True")

ARGS.parser.add_argument(
    "--lunsize", type=str,
    default="20g",
    help="Size of luns to be created. Default: 20g")

ARGS.parser.add_argument(
    "--datastoregrow", type=str,
    default="3g",
    help="Datastore size to grow. Default: '3g'")

ARGS.parser.add_argument(
    "--sourcevm", type=str,
    default="damn-small-linux-scsi",
    help="Name of source VM. Default: 'damn-small-linux-scsi'")

ARGS.parser.add_argument(
    "--ovapackage", type=str,
    default="Marsdev-1.5.4.esx.ova",
    help="Name of OVA file. Default: 'Marsdev-1.5.4.esx.ova'")

ARGS.parser.add_argument(
    "--clonenumber", type=int,
    default=5,
    help="Number of VMs to be cloned. Default: 5")


class ESXHostAPI(FRTestCase):
    def suiteSetup(self):
        self.lunSize = ARGS.values.lunsize
        self.datastoreGrow = ARGS.values.datastoregrow

        self.sourceHost = ARGS.values.sourcehost
        self.sourcePath = ARGS.values.sourcepath
        self.sourceVM = ARGS.values.sourcevm
        self.ovaPackage = ARGS.values.ovapackage
        self.cloneNumber = ARGS.values.clonenumber

        self.cleanupMars = ARGS.values.cleanupmars
        self.marsArr = express.Utility.getMars()
        self.node = self.marsArr[0]

        self.esxHostName = ARGS.values.esxhost
        self.esxHost = express.Hosts(hosts=[self.esxHostName])[self.esxHostName]

        self.fcInitiators = self._getInitiators(node=self.node, host=self.esxHost)

        if not self.esxHost.getDatastores(names=['template_vm']):

            self.esxHost.createFS(name='template_vm', remoteHost=self.sourceHost, remotePath=self.sourcePath,
            method='NAS')
        self.sourceDatastore = self.esxHost.getDatastores(names=['template_vm'])[0]

    def testSetup(self):
        LOG.step('Loading existing LUNs')
        self.luns = express.Luns(node=self.node, cleanup=True)

        self.igroups = express.Igroups(node=self.node, cleanup=self.cleanupMars)
        self.igroups.create(count=1, ostype='vmware', initiators=self.fcInitiators, prefix='ig_esx')

    def test_esx_host_system(self):
        LOG.step('Checking ESXi host alive')
        self.assertTrue(self.esxHost.isAlive())
        LOG.info('ESXi host alive:', self.esxHost.isAlive())

        LOG.step('Getting host version')
        esxHostVersion = self.esxHost.getVersion()
        self.assertTrue('VMware ESXi' in esxHostVersion)
        LOG.info('ESXI host version:', esxHostVersion)

        LOG.step('Getting ESXi host type')
        esxHostType = self.esxHost.getHostType()
        self.assertTrue(esxHostType == 'esx')
        LOG.info('ESXi host type:', esxHostType)

    def test_esx_host_device(self):
        LOG.step('Setting up LUNs/igroups')
        self.luns.create(count=2, size=self.lunSize, prefix='lun_esx')

        for lun in self.luns:
            lun += self.igroups[0]
        LOG.info('Created and mapped LUNs')

        LOG.step('Rescanning HBAs')
        self.esxHost.rescan()
        LOG.info('Rescanned HBAs.')

        LOG.step('Getting FC initiators')
        fcs = self.esxHost.getFCInitiators()
        LOG.info('FC initiators info:\n', fcs)

        LOG.step('Getting specified FC initiators')
        fc = self.esxHost.getFCInitiators(matchWWPN=self.fcInitiators)[0]
        self.assertTrue(fc['WWPN'] in self.fcInitiators)
        LOG.info('FC initiator info:\n', fc)

        LOG.step('Getting storage devices')
        luns = self.esxHost.getDevices(uniqLuns=True, node=self.node)
        for lun in luns:
            self.assertTrue(lun['model'] == 'LUN FlashRay')
            self.assertTrue(lun['device-type'] == 'disk')
            self.assertTrue(lun['scsiLevel'] == 4)
            self.assertTrue(lun['protocol'] == 'FCP')
            self.assertTrue(lun['lun-type'] == 'disk')
            self.assertTrue(lun['state'] == 'ok')
            self.assertTrue(lun['vendor'] == 'NETAPP')
            LOG.info('Storage devices:\n', lun)

    def test_esx_host_datastore(self):
        LOG.step('Setting up LUNs/igroups')
        self.luns.create(count=3, size=self.lunSize, prefix='lun_esx')
        for lun in self.luns:
            lun += self.igroups[0]
        self.esxHost.rescan()
        LOG.info('Created and mapped LUNs.')

        devices = self.esxHost.getDevices(node=self.node, luns=[i.name for i in self.luns])

        LOG.step('Creating LUN-backed datastore at full capacity of LUN')
        datastoreName0 = 'DS_' + self.esxHost._getTimestampID()
        datastore0 = self.esxHost.createFS(name=datastoreName0, device=devices[0]['device'])
        self.assertTrue(datastore0['name'] == datastoreName0)
        originalCapacity = datastore0['capacity']
        LOG.info("Created datastore '%s' with capacity %s bytes." % (datastore0['name'], datastore0['capacity']))

        LOG.step('Extending datastore by adding space on additional LUN')
        self.esxHost.extendDatastore(name=datastoreName0, device=devices[1]['device'], size=self.datastoreGrow)
        datastore0 = self.esxHost.getDatastores(names=[datastoreName0])[0]

        self.assertTrue(datastore0['capacity'] > originalCapacity)
        LOG.info('Datastore extended to size %s bytes.' % datastore0['capacity'])

        LOG.step('Creating LUN-backed datastore at size 4096 MB')
        datastoreName1 = 'DS_' + self.esxHost._getTimestampID()
        datastore1 = self.esxHost.createFS(name=datastoreName1, device=devices[2]['device'], size='4096M')

        self.assertTrue(datastore1['capacity'] > 0)
        LOG.info("Created datastore '%s' with capacity %s." % (datastore1['name'], datastore1['capacity']))

        LOG.step('Removing datastores from datacenter')
        self.esxHost.removeDatastore(name=datastore0['name'])
        self.esxHost.removeDatastore(name=datastore1['name'])
        self.assertFalse(self.esxHost.getDatastores(names=[datastore0['name'], datastore1['name']]))

    def test_esx_host_create_vm(self):
        LOG.step('Setting up LUNs/igroups')
        self.luns.create(count=1, size=self.lunSize, prefix='lun_esx')
        self.luns[0] += self.igroups[0]
        device = self.esxHost.getDevices(serial=self.luns[0].serial.encode('utf-8'))[0]['lun-pathname']
        LOG.info("Created and mapped LUN '%s'", device)

        LOG.step('Creating datastore')
        datastoreName = 'DS_' + self.esxHost._getTimestampID()
        datastore = self.esxHost.createFS(name=datastoreName, device=device)
        LOG.info('Datastore is: ', datastore)

        LOG.step('Creating virtual machine')
        vmName = 'VM_' + str(uuid.uuid4())[:8]
        vm = self.esxHost.createVM(datastore=datastoreName, vmPrefix=vmName, cpu=2,
        memory='2048M', guestID='debian5Guest')
        self.assertTrue(vm.name == vmName)
        self.assertTrue(vm.cpu == 2)
        self.assertTrue(vm.memory == int(utils.convertBytes(size='2048M')['B']))
        self.assertTrue(vm.guestID == 'debian5Guest')

        LOG.step('Deleting datastore with virtual machine')
        self.esxHost.removeDatastore(name=datastoreName, force=True)
        self.assertFalse(self.esxHost.getDatastores(names=[datastoreName]))
        LOG.info("Removed datastore '%s'." % datastoreName)

    def test_esx_host_clone_vm(self):
        LOG.step('Setting up LUNs/igroups')
        self.luns.create(count=1, size=self.lunSize, prefix='lun_esx')
        self.luns[0] += self.igroups[0]
        device = self.esxHost.getDevices(serial=self.luns[0].serial.encode('utf-8'))[0]['lun-pathname']
        LOG.info("Created and mapped LUN '%s'" % device)

        LOG.step('Creating datastore')
        datastoreName = 'DS_' + self.esxHost._getTimestampID()
        datastore = self.esxHost.createFS(name=datastoreName, device=device)
        LOG.info('Created datastore:\n', datastore)

        LOG.step('Cloning virtual machine')
        vm = self.esxHost.cloneVM(sourceDatastore=self.sourceDatastore['name'], sourceVM=self.sourceVM,
        targetDatastore=datastore['name'])[0]
        LOG.info('Cloned virtual machine.')

        LOG.step("Cloning %s virtual machines on the same datastore" % self.cloneNumber)
        oldVMs = self.esxHost.getVMs()

        vms = self.esxHost.cloneVM(sourceDatastore=datastoreName, sourceVM=vm.name, targetDatastore=datastore['name'],
        count=self.cloneNumber)
        self.assertTrue(len(self.esxHost.getVMs()) == len(oldVMs) + self.cloneNumber)
        LOG.info("Cloned %s virtual machines." % self.cloneNumber)

        LOG.step("Cloning %s virtual machines with overridden operator '*='" % self.cloneNumber)
        self.esxHost.getVMs()[vm.name] *= self.cloneNumber
        self.assertTrue(len(self.esxHost.getVMs()) == len(oldVMs) + (self.cloneNumber * 2))
        LOG.info("Cloned %s virtual machines." % self.cloneNumber)

        LOG.step('Deleting cloned virtual machines')
        vmNames = [v.name for v in vms]
        for loop in range(len(vms)):
            vms[loop].delete()
        currentVMNames = [v.name for v in self.esxHost.getVMs()]
        for vmName in vmNames:
            self.assertFalse(vmName in currentVMNames)
        LOG.info('Deleted virtual machines.')

        LOG.step('Deleting datastore with virtual machines')
        self.esxHost.removeDatastore(name=datastore['name'], force=True)
        self.assertFalse(self.esxHost.getDatastores(names=[datastore['name']]))
        LOG.info('Removed datstore.')

    def test_esx_host_deploy_ova(self):
        LOG.step('Setting up LUNs/igroups')
        self.luns.create(count=1, size='3t', prefix='lun_esx')
        self.luns[0] += self.igroups[0]
        device = self.esxHost.getDevices(serial=self.luns[0].serial.encode('utf-8'))[0]['lun-pathname']
        LOG.info("Created and mapped LUN '%s'", device)

        LOG.step('Creating datastore')
        datastoreName = 'DS_' + self.esxHost._getTimestampID()
        datastore = self.esxHost.createFS(name=datastoreName, device=device)
        LOG.info('Datastore created is: ', datastore)

        LOG.step("Deploying OVA package %s times" % self.cloneNumber)
        self.esxHost.deployOVA(ovaName=self.ovaPackage, datastore=datastore['name'], count=self.cloneNumber)
        self.assertTrue(len(self.esxHost.getVMs()) == self.cloneNumber)

        LOG.step("Deleting datastore with %s virtual machines" % self.cloneNumber)
        self.esxHost.removeDatastore(name=datastore['name'], force=True)
        self.assertFalse(self.esxHost.getDatastores(names=[datastore['name']]))
        LOG.info('Removed datastore.')

    def test_esx_host_power_vm(self):
        LOG.step('Setting up LUNs/igroups')
        self.luns.create(count=1, size=self.lunSize, prefix='lun_esx')
        self.luns[0] += self.igroups[0]
        device = self.esxHost.getDevices(serial=self.luns[0].serial.encode('utf-8'))[0]['lun-pathname']
        LOG.info("Created and mapped LUN '%s'", device)

        LOG.step('Creating datastore')
        datastoreName = 'DS_' + self.esxHost._getTimestampID()
        datastore = self.esxHost.createFS(name=datastoreName, device=device)
        LOG.info('Datastore created is: ', datastore)

        LOG.step('Cloning virtual machine')
        vm = self.esxHost.cloneVM(sourceDatastore=self.sourceDatastore['name'], sourceVM=self.sourceVM,
        targetDatastore=datastoreName)[0]
        LOG.info('Created virtual machine.')

        LOG.step('Powering on virtual machine')
        vm.powerOn()
        self.assertTrue(vm.powerState == 'poweredOn')
        LOG.info('Powered on virtual machine.')

        LOG.step('Suspending virtual machine')
        vm.suspend()
        self.assertTrue(vm.powerState == 'suspended')
        LOG.info('Suspended virtual machine.')

        LOG.step('Powering off virtual machine')
        vm.powerOff()
        self.assertTrue(vm.powerState == 'poweredOff')
        LOG.info('Powered off virtual machine.')

        LOG.step('Removing datastore with running virtual machine')
        vm.powerOn()
        self.assertTrue(vm.powerState == 'poweredOn')
        self.esxHost.removeDatastore(name=datastoreName, force=True)
        self.assertFalse(self.esxHost.getDatastores(names=[datastoreName]))
        LOG.info('Removed datastore.')

    def test_esx_host_reconfigure_vm(self):
        LOG.step('Setting up LUNs/igroups')
        self.luns.create(count=2, size=self.lunSize, prefix='lun_esx')
        self.luns[0] += self.igroups[0]
        self.luns[1] += self.igroups[0]

        device1 = self.esxHost.getDevices(serial=self.luns[0].serial.encode('utf-8'))[0]['lun-pathname']
        device2 = self.esxHost.getDevices(serial=self.luns[1].serial.encode('utf-8'))[0]['lun-pathname']
        LOG.info('Created and mapped LUNs:', device1, device2)

        LOG.step('Creating datastore')
        datastoreName = 'DS_' + self.esxHost._getTimestampID()
        datastore = self.esxHost.createFS(name=datastoreName, device=device1)
        LOG.info('Datastore created is: ', datastore)

        LOG.step('Cloning virtual machine')
        vm = self.esxHost.cloneVM(sourceDatastore=self.sourceDatastore['name'], sourceVM=self.sourceVM,
        targetDatastore=datastoreName)[0]
        LOG.info('Created virtual machine.')

        LOG.step('Powering on virtual machine')
        vm.powerOn()

        self.assertTrue(vm.powerState == 'poweredOn')
        LOG.info('Powered on virtual machine.')

        LOG.step('Changing number of CPUs and memory size')
        vm.reconfigure(cpu=4, memory='4096M', force=True)
        self.assertTrue(vm.cpu == 4)
        self.assertTrue(vm.memory == int(utils.convertBytes(size='4096M')['B']))
        LOG.info('Reconfigured virtual machine.')

        LOG.step('Adding local disk to virtual machine')
        newDisk1 = vm.addDisk(size='2048M')
        self.assertTrue(newDisk1['diskID'] in [disk['diskID'] for disk in vm.getDisks()])
        self.assertTrue(newDisk1['capacity'] == int(utils.convertBytes(size='2048M')['B']))
        LOG.info('Added local disk to virtual machine:\n', {key: newDisk1[key] for key in newDisk1 if key !=
        '_vimObject'})

        LOG.step('Adding LUN-backed disk to virtual machine')
        newDisk2 = vm.addDisk(device=device2)
        self.assertTrue(newDisk2['diskID'] in [disk['diskID'] for disk in vm.getDisks()])
        LOG.info('Added LUN-backed disk to virtual machine:\n', {key: newDisk2[key] for key in newDisk2 if key !=
        '_vimObject'})

        LOG.step('Removing disks from virtual machine')
        vm.removeDisk(diskID=newDisk1['diskID'])
        self.assertFalse(newDisk1['diskID'] in [disk['diskID'] for disk in vm.getDisks()])
        vm.removeDisk(diskID=newDisk2['diskID'])
        self.assertFalse(newDisk2['diskID'] in [disk['diskID'] for disk in vm.getDisks()])
        LOG.info('Removed disks from virtual machine.')
        self.esxHost.removeDatastore(name=datastoreName, force=True)
        LOG.info('Removed datastore with running virtual machine.')

    def testTeardown(self):
        try:
            LOG.info("Destroying existing LUNs and igroups...")
            del self.igroups
            del self.luns
        except Exception, e:
            raise FailedProductException(e)
        self.esxHost.rescan()

    def _getInitiators(self, node, host):
        lifInits = node.fcp.initiator_show(json=True)
        if not len(lifInits):
            raise FailedConfigException('FCP Initiators are not found')
        fcpWWPN = [lif['wwpn'] for lif in lifInits]
        return ','.join([i['WWPN'] for i in host.getFCInitiators(matchWWPN=fcpWWPN)])

if __name__ == '__main__':
    ARGS.parseArgs(purpose)
    esx_host_api_unit = ESXHostAPI()
    sys.exit(esx_host_api_unit.numberOfFailedTests())
