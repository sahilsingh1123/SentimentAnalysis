#todo you can look for the python documentatio if needed for the assertiton

import unittest
import TestUnitTesting

'''
note- to run this test
you need to run through terminal with command
python -m unittest test_class.py

or to run from the code editor
if __name__ == "__main__":
    unittest.main()
'''

class TestCalc(unittest.TestCase):

    # every method needs to start with "test_"
    def test_add(self):
        result = TestUnitTesting.add(10,5)
        self.assertEqual(result, 15)
        self.assertEqual(TestUnitTesting.add(-10,5), -5)
        self.assertEqual(TestUnitTesting.add(-10,-5), -15)

    def test_sub(self):
        result = TestUnitTesting.subtract(10,5)
        self.assertEqual(result, 5)

    def test_divide(self):
        self.assertEqual(TestUnitTesting.divide(10,2), 5)
        with self.assertRaises(ValueError):
            TestUnitTesting.divide(10,0)



if __name__ == "__main__":
    unittest.main()
