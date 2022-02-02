import geopandas
import glob
import os.path
import json
import sqlalchemy
import jsonschema

script_path = os.path.dirname(os.path.realpath(__file__))

def sql_url(user, passwd, url, db):
     return "postgresql://{user}:{passwd}@{url}/{db}".format(user=user, passwd=passwd, url=url, db=db)

def get_safe_engine(user, passwd, url, db):
    engine = sqlalchemy.create_engine(sql_url(user, passwd, url, db))
    try:
        connection = engine.connect()
        connection.close()
    except Exception as e:
        print(e)
        return False
    return engine

def valid_db_config(ifolder):
    config_schema = None
    with open(os.path.join(script_path, "schemas/db_schema/schema.json")) as schema:
        config_schema = json.load(schema)
    for config_file in glob.glob(os.path.join(ifolder, "**/dbs.json")):
        data = None
        with open(config_file) as json_file:
            data = json.load(json_file)
        jsonschema.validate(data, config_schema)

def valid_shp_config(ifolder):
    with open(os.path.join(script_path, "schemas/shp_config_schema/schema.json")) as schema:
        config_schema = json.load(schema)
    for i in glob.glob(os.path.join(ifolder, "**/*.shp"), recursive=True):
        with open("{}.{}".format(os.path.splitext(i)[0], "config")) as file_shp_config:
            shp_config = json.load(file_shp_config)
        jsonschema.validate(shp_config, config_schema)

def get_config(ifolder):
    config = {}
    for config_file in glob.glob(os.path.join(ifolder, "**/dbs.json")):
        with open(config_file) as json_file:
            data = json.load(json_file)
        for db in data:
            if 'password' not in data[db]:
                engine = False
                while not engine:
                    passwd = input()
                    engine = get_safe_engine(data[db]['user'], passwd, data[db]['url'], data[db]['db'])
            else:
                engine = get_safe_engine(data[db]['user'], data[db]['passwd'], data[db]['url'], data[db]['db'])
                if not engine:
                    print("In the config file: {}".format(config_file))
                    print("Could not connect to the db: {}".format(db))
                    raise NameError("Connection Error")
            config[db] = engine
    return config

def shp2postgis(ifolder):
    valid_db_config(ifolder)
    valid_shp_config(ifolder)
    config = get_config(ifolder)
    for i in glob.glob(os.path.join(ifolder, "**/*.shp"), recursive=True):
        shape = geopandas.read_file(i)
        with open("{}.{}".format(os.path.splitext(i)[0], "config")) as file_shp_config:
            shp_config = json.load(file_shp_config)
        shape.to_postgis(shp_config["name"], config[shp_config["db"]])

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description = 'import shape files massively to postgis')
    parser.add_argument('folder', help='input folder')
    args = parser.parse_args()
    if os.path.isdir(parser.folder):
        shp2postgis(parser.folder)
    else:
        raise NameError("No input folder")
