import unittest
import os
import jsonschema
import json

class Schemas(unittest.TestCase):
    def test_valids(self):
        for dir_schema in os.listdir("schemas"):
            #Needed this line?
            schema = None
            with open(os.path.join("schemas", dir_schema, "schema.json")) as jschema:
                schema = json.load(jschema)
            for ftest in os.listdir(os.path.join("schemas", dir_schema, "valid")):
                test = open(os.path.join("schemas", dir_schema, "valid", ftest))
                data = json.load(test)
                test.close()
                try:
                    jsonschema.validate(data, schema)
                except:
                    #print("The file {}/{}/{}".format(dir_schema, "valid", ftest))
                    #print("Shoud be a valid json")
                    self.fail("Wrong json scheme or file structure")
    def test_invalids(self):
        for dir_schema in os.listdir("schemas"):
            #Needed this line?
            schema = None
            with open(os.path.join("schemas", dir_schema, "schema.json")) as jschema:
                schema = json.load(jschema)
            for ftest in os.listdir(os.path.join("schemas", dir_schema, "invalid")):
                test = open(os.path.join("schemas", dir_schema, "invalid", ftest))
                data = json.load(test)
                test.close()
                with self.assertRaises(Exception):
                    jsonschema.validate(data, schema)

if __name__ == '__main__':
    unittest.main()