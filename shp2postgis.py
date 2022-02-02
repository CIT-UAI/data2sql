import geopandas
import glob
import os.path
import json
import sqlalchemy
import jsonschema

script_path = os.path.dirname(os.path.realpath(__file__))

debug = True

def replace_keys(shp_config, keys):
    for option in shp_config:
        tmp = shp_config[option]
        for key in keys:
            tmp = tmp.replace(key, keys[key])
        shp_config[option] = tmp
    return shp_config

def sql_url(user, password, url, db):
     return "postgresql://{user}:{password}@{url}/{db}".format(user=user, password=password, url=url, db=db)

def get_safe_engine(user, password, url, db):
    engine = sqlalchemy.create_engine(sql_url(user, password, url, db))
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
    for config_file in glob.glob(os.path.join(ifolder, "**/dbs.json"), recursive=True):
        data = None
        with open(config_file) as json_file:
            data = json.load(json_file)
        jsonschema.validate(data, config_schema)

def valid_shp_config(ifolder):
    with open(os.path.join(script_path, "schemas/shp_config_schema/schema.json")) as schema:
        config_schema = json.load(schema)
    for i in glob.glob(os.path.join(ifolder, "**/*.shp"), recursive=True):
        if not os.path.exists("{}.{}".format(os.path.splitext(i)[0], "json")):
            continue
        with open("{}.{}".format(os.path.splitext(i)[0], "json")) as file_shp_config:
            shp_config = json.load(file_shp_config)
        jsonschema.validate(shp_config, config_schema)

def get_config(ifolder):
    config = {}
    for config_file in glob.glob(os.path.join(ifolder, "**/dbs.json"), recursive=True):
        with open(config_file) as json_file:
            data = json.load(json_file)
        for db in data:
            if 'password' not in data[db]:
                engine = False
                while not engine:
                    password = input()
                    engine = get_safe_engine(data[db]['user'], password, data[db]['url'], data[db]['db'])
            else:
                engine = get_safe_engine(data[db]['user'], data[db]['password'], data[db]['url'], data[db]['db'])
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
        if not os.path.exists("{}.{}".format(os.path.splitext(i)[0], "json")):
            continue
        shape = geopandas.read_file(i)
        with open("{}.{}".format(os.path.splitext(i)[0], "json")) as file_shp_config:
            shp_config = json.load(file_shp_config)
        rep_keys = {}
        rep_keys["{file_name}"] = os.path.basename(i)
        rep_keys["{file_name_no_ext}"] = os.path.splitext(rep_keys["{file_name}"])[0]
        shp_config = replace_keys(shp_config, rep_keys)
        geopandas_params_keys = ["if_exists",
                                "schema",
                                "index",
                                "index_label",
                                "chunksize",
                                "dtype"]
        geopandas_params = {}
        for p in geopandas_params_keys:
            if p in shp_config:
                geopandas_params[p] = shp_config[p]
        if debug:
            geopandas_params["if_exists"] = "replace"
        print(shp_config)
        print("importing to postgis")
        shape.to_postgis(shp_config["name"], config[shp_config["db"]], **geopandas_params)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description = 'import shape files massively to postgis')
    parser.add_argument('folder', help='input folder')
    args = parser.parse_args()
    if os.path.isdir(args.folder):
        shp2postgis(args.folder)
    else:
        raise NameError("No input folder")
