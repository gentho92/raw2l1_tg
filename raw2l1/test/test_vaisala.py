import os
import subprocess
import unittest

import netCDF4 as nc
import numpy as np

MAIN_DIR = os.path.dirname(os.path.dirname(__file__)) + os.sep
TEST_DIR = os.path.join(MAIN_DIR, "test")
CONF_DIR = os.path.join(TEST_DIR, "conf")
TEST_IN_DIR = os.path.join(TEST_DIR, "input")
TEST_OUT_DIR = os.path.join(TEST_DIR, "output")
PRGM = "raw2l1.py"


class TestVaisalaCL31(unittest.TestCase):
    IN_DIR = os.path.join(TEST_IN_DIR, "vaisala_cl31")
    conf_file = os.path.join(CONF_DIR, "conf_vaisala_cl31_eprofile.ini")

    def test_cl31_onehour_file(self):
        date = "20141030"
        test_ifile = os.path.join(self.IN_DIR, "cl31_0a_z1R5mF3s_v01_20141030_*.asc")
        test_ofile = os.path.join(TEST_OUT_DIR, "test_cl31_20141030_000002.nc")

        resp = subprocess.check_call(
            [
                os.path.join(MAIN_DIR, PRGM),
                date,
                self.conf_file,
                test_ifile,
                test_ofile,
                "-log_level",
                "debug",
            ]
        )

        self.assertEqual(resp, 0, "CL31 one hour file")


class TestVaisalaMsg2(unittest.TestCase):
    IN_DIR = os.path.join(TEST_IN_DIR, "vaisala_cl")
    conf_file = os.path.join(CONF_DIR, "conf_vaisala_cl31_eprofile.ini")

    def test_cl_msg2(self):
        date = "20150617"
        test_ifile = os.path.join(self.IN_DIR, "vaisala_cl_msg2.txt")
        test_ofile = os.path.join(TEST_OUT_DIR, "test_cl31_20150617_000000.nc")

        resp = subprocess.check_call(
            [
                os.path.join(MAIN_DIR, PRGM),
                date,
                self.conf_file,
                test_ifile,
                test_ofile,
                "-log_level",
                "debug",
            ]
        )

        self.assertEqual(resp, 0, "CL31 one hour file")

    def test_cl_scale_error(self):
        date = "20150617"
        test_ifile = os.path.join(self.IN_DIR, "vaisala_cl_scale_error.txt")
        test_ofile = os.path.join(
            TEST_OUT_DIR, "test_cl31-scale-error_20150617_000000.nc"
        )

        resp = subprocess.check_call(
            [
                os.path.join(MAIN_DIR, PRGM),
                date,
                self.conf_file,
                test_ifile,
                test_ofile,
                "-log_level",
                "debug",
            ]
        )

        self.assertEqual(resp, 0, "CL31 one hour file")


class TestVaisalaCL51(unittest.TestCase):
    IN_DIR = os.path.join(TEST_IN_DIR, "vaisala_cl51")
    conf_file = os.path.join(CONF_DIR, "conf_vaisala_cl51_eprofile.ini")

    def test_cl51_oneday_file(self):
        date = "20140901"
        test_ifile = os.path.join(self.IN_DIR, "h4090100.dat")
        test_ofile = os.path.join(TEST_OUT_DIR, "test_cl51_20140901.nc")

        resp = subprocess.check_call(
            [
                os.path.join(MAIN_DIR, PRGM),
                date,
                self.conf_file,
                test_ifile,
                test_ofile,
                "-log_level",
                "debug",
            ]
        )

        self.assertEqual(resp, 0, "CL51 one hour file")


class TestVaisalaSwissAirport(unittest.TestCase):
    IN_DIR = os.path.join(TEST_IN_DIR, "vaisala_cl")
    conf_file = os.path.join(CONF_DIR, "conf_vaisala_cl31-swiss-airports_eprofile.ini")

    def test_2_files(self):
        date = "20150819"
        test_ifile = os.path.join(self.IN_DIR, "20150819*.log")
        test_ofile = os.path.join(TEST_OUT_DIR, "test_cl-swiss-airport_20150819.nc")

        resp = subprocess.check_call(
            [
                os.path.join(MAIN_DIR, PRGM),
                date,
                self.conf_file,
                test_ifile,
                test_ofile,
                "-log_level",
                "debug",
                "-v",
                "debug",
            ]
        )

        self.assertEqual(resp, 0, "CL swiss airport")


class TestVaisalaBugSIRTA(unittest.TestCase):
    IN_DIR = os.path.join(TEST_IN_DIR, "vaisala_cl31")
    conf_file = os.path.join(CONF_DIR, "conf_vaisala_cl31_eprofile.ini")

    def test_20150911(self):
        date = "20150911"
        test_ifile = os.path.join(self.IN_DIR, "cl31_0a_z1R5mF3s_v02_20150911_*.asc")
        test_ofile = os.path.join(TEST_OUT_DIR, "test_cl-sirta_20150911.nc")

        resp = subprocess.check_call(
            [
                os.path.join(MAIN_DIR, PRGM),
                date,
                self.conf_file,
                test_ifile,
                test_ofile,
                "-log_level",
                "debug",
                "-v",
                "debug",
            ]
        )

        self.assertEqual(resp, 0, "CL SIRTA bug message type")

    def test_20150521(self):
        date = "20150521"
        test_ifile = os.path.join(self.IN_DIR, "cl31_0a_z1R5mF3s_v02_20150521_*.asc")
        test_ofile = os.path.join(TEST_OUT_DIR, "test_cl-sirta_20150521.nc")

        resp = subprocess.check_call(
            [
                os.path.join(MAIN_DIR, PRGM),
                date,
                self.conf_file,
                test_ifile,
                test_ofile,
                "-log_level",
                "debug",
                "-v",
                "debug",
            ]
        )

        self.assertEqual(resp, 0, "CL SIRTA bug cbh full obscuration determined")

    def test_20150603(self):
        date = "20150603"
        test_ifile = os.path.join(self.IN_DIR, "cl31_0a_z1R5mF3s_v02_20150603_*.asc")
        test_ofile = os.path.join(TEST_OUT_DIR, "test_cl-sirta_20150603.nc")

        resp = subprocess.check_call(
            [
                os.path.join(MAIN_DIR, PRGM),
                date,
                self.conf_file,
                test_ifile,
                test_ofile,
                "-log_level",
                "debug",
                "-v",
                "debug",
            ]
        )

        self.assertEqual(resp, 0, "CL SIRTA bug conversion hexadecimal data")


class TestUnitsInFeet(unittest.TestCase):
    """Test the convertion of data from feet to meters"""

    IN_DIR = os.path.join(TEST_IN_DIR, "vaisala_cl")

    def test_cl31_feet(self):
        """test using data from station 08360 in feet"""

        wanted_cbh_values = [
            7397,
            7562,
            7598,
            7580,
            7388,
            7406,
            7479,
            7620,
            7443,
            7620,
            7488,
            7415,
            7507,
            7452,
            7562,
            7562,
            7620,
            7620,
            7525,
            7525,
        ]

        date = "20161113"
        conf_file = os.path.join(CONF_DIR, "conf_vaisala_cl31_eprofile.ini")
        test_ifile = os.path.join(
            self.IN_DIR,
            "ceilometer-eprofile_20161113233608_08045_A201611132320_cl31.dat",
        )
        test_ofile = os.path.join(TEST_OUT_DIR, "test_cbh_feet.nc")

        subprocess.check_call(
            [
                MAIN_DIR + PRGM,
                date,
                conf_file,
                test_ifile,
                test_ofile,
                "-log_level",
                "debug",
                "-v",
                "debug",
            ]
        )

        # get cbh values
        nc_id = nc.Dataset(test_ofile)
        raw2l1_cbh = np.ma.filled(nc_id.variables["cloud_base_height"][:])
        nc_id.close()

        self.assertEqual(
            wanted_cbh_values,
            raw2l1_cbh[:, 0].flatten().astype(int).tolist(),
            "test cbh CL31 conversion from feet to meters",
        )

    def test_cl31_meters(self):
        """test using data from station 08360 in feet"""

        wanted_cbh_values = [3450, 3455, 3455, 3455, 3460, 3460, 3465, 3470, 3470, 3470]

        date = "20141030"
        conf_file = os.path.join(CONF_DIR, "conf_vaisala_cl31_eprofile.ini")
        test_ifile = os.path.join(self.IN_DIR, "vaisala_test_cbh_meters.dat")
        test_ofile = os.path.join(TEST_OUT_DIR, "test_cbh_meters.nc")

        subprocess.check_call(
            [
                MAIN_DIR + PRGM,
                date,
                conf_file,
                test_ifile,
                test_ofile,
                "-log_level",
                "debug",
                "-v",
                "debug",
            ]
        )

        # get cbh values
        nc_id = nc.Dataset(test_ofile)
        raw2l1_cbh = np.ma.filled(nc_id.variables["cloud_base_height"][:])
        nc_id.close()

        self.assertEqual(
            wanted_cbh_values,
            raw2l1_cbh[:, 0].flatten().astype(int).tolist(),
            "test cbh CL31 in meters no conversion needed",
        )


class TestVaisalaCL31Belgium(unittest.TestCase):
    IN_DIR = os.path.join(TEST_IN_DIR, "vaisala_cl31")
    conf_file = os.path.join(CONF_DIR, "conf_vaisala_cl31_eprofile.ini")

    def test_cl31_belgium_file(self):
        date = "20220119"
        test_ifile = os.path.join(
            self.IN_DIR, "06496_A202201191200_cl31_belgium-fmt.DAT"
        )
        test_ofile = os.path.join(
            TEST_OUT_DIR, "06496_A202201191200_cl31_belgium-fmt.nc"
        )

        resp = subprocess.check_call(
            [
                os.path.join(MAIN_DIR, PRGM),
                date,
                self.conf_file,
                test_ifile,
                test_ofile,
                "-log_level",
                "debug",
            ]
        )

        self.assertEqual(resp, 0, "CL51 one hour file")


if __name__ == "__main__":
    unittest.main()
