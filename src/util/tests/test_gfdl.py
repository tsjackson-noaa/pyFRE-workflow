import os
import sys
import unittest
import subprocess
import src.util.gfdl_util as gfdl_util


class TestModuleManager(unittest.TestCase):

    test_mod_name = 'latexdiff/1.2.0' # least likely to cause side effects?

    def setUp(self):
        _ = gfdl_util.ModuleManager()

    def tearDown(self):
        # call _reset method clearing ModuleManager for unit testing,
        # otherwise the second, third, .. tests will use the instance created
        # in the first test instead of being properly initialized
        temp = gfdl_util.ModuleManager()
        temp.revert_state()
        temp._reset()

    def test_module_envvars(self):
        self.assertIn('MODULESHOME', os.environ)
        self.assertIn('MODULE_VERSION', os.environ)
        self.assertIn('LOADEDMODULES', os.environ)
        self.assertEqual(
            '/usr/local/Modules/'+os.environ['MODULE_VERSION'],
            os.environ['MODULESHOME']
        )

    def test_module_avail(self):
        cmd = '{}/bin/modulecmd'.format(os.environ['MODULESHOME'])
        for mod in gfdl._current_module_versions.values():
            # module list writes to stderr, because all module user output does
            list1 = subprocess.check_output([cmd, 'python', 'avail', '-t', mod],
                stderr=subprocess.STDOUT).splitlines()
            list1 = [s for s in list1 if not s.endswith(':')]
            self.assertNotEqual(list1, [],
                msg='No module {}'.format(mod))

    def test_module_list(self):
        cmd = '{}/bin/modulecmd'.format(os.environ['MODULESHOME'])
        # module list writes to stderr, because all module user output does
        list1 = subprocess.check_output([cmd, 'python', 'list', '-t'],
            stderr=subprocess.STDOUT).splitlines()
        del list1[0]
        list1 = set([s.replace('(default)','') for s in list1])
        modMgr = gfdl_util.ModuleManager()
        list2 = set(modMgr._list())
        self.assertEqual(list1, list2)

    def test_module_load(self):
        modMgr = gfdl_util.ModuleManager()
        modMgr.load(self.test_mod_name)
        mod_list = modMgr._list()
        self.assertIn(self.test_mod_name, mod_list)

    def test_module_unload(self):
        modMgr = gfdl_util.ModuleManager()
        modMgr.load(self.test_mod_name)
        mod_list = modMgr._list()
        self.assertIn(self.test_mod_name, mod_list)
        modMgr.unload(self.test_mod_name)
        mod_list = modMgr._list()
        self.assertNotIn(self.test_mod_name, mod_list)


if __name__ == '__main__':
    unittest.main()
