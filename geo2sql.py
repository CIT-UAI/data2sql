import geopandas
import glob
import os.path
import json
import sqlalchemy
import jsonschema
import pathlib

script_path = os.path.dirname(os.path.realpath(__file__))

def get_safe_engine(conn):
    engine = sqlalchemy.create_engine(conn)
    connection = engine.connect()
    connection.close()
    return engine

def replace_keys(geo_config, keys):
    for option in geo_config:
        if not isinstance(geo_config[option], str):
            continue
        tmp = geo_config[option]
        for key in keys:
            tmp = tmp.replace(key, keys[key])
        geo_config[option] = tmp
    return geo_config

def fast_json(file):
    with open(file) as f:
        data = json.load(f)
    return data

def join2configs(config1, config2):
    if 'mix' in config2 and config2['mix'] == 'clean':
        return config2
    if ('mix' in config2 and config2['mix'] == 'replace') or \
        'mix' not in config2:
        ret = config1.copy()
        if 'mix' in ret:
            del ret['mix']
        for i in config2:
            config1[i] = config2[i]
        return ret
    raise NameError("Not supported way to join configs: {}".format(config2['mix']))

def get_file_config(geo_file_path, config):
    file_config = fast_json(geo_file_path.with_suffix(".json"))
    if 'mix' in file_config and file_config['mix'] == 'clean':
        return file_config
    _ = os.path.split(geo_file_path)
    ret = {}
    conn = None
    for _dir in range(len(_)-1):
        pconfig = os.path.join(*_[0:_dir], "config.json")
        if pconfig in config:
            ret = join2configs(ret, config[pconfig]['default'])
            if file_config['db'] in config[pconfig]['dbs']:
                conn = config[pconfig]['dbs'][file_config['db']]
    if conn is None:
        raise NameError("The DB {} was not found".format(file_config['db']))
    return conn, ret

def pandas2sql(geo_file_path, config):
    try:
        geo_file = geopandas.read_file(geo_file_path)
    except Exception as e:
        print(e)
        raise NameError("The file {}\n can't be loaded".format(geo_file_path))
    conn, geo_config = get_file_config(geo_file_path, config)
    rep_keys = {}
    rep_keys["{file_name}"] = geo_file_path.name
    rep_keys["{file_name_no_ext}"] = geo_file_path.stem
    geo_config = replace_keys(geo_config, rep_keys)
    geopandas_params_keys = ["if_exists",
                            "schema",
                            "index",
                            "index_label",
                            "chunksize",
                            "dtype"]
    geopandas_params = {}
    for p in geopandas_params_keys:
        if p in geo_config:
            geopandas_params[p] = geo_config[p]
    if __debug__:
        geopandas_params["if_exists"] = "replace"
    if __debug__:
        print("shape config {}".format(geo_file_path))
        print(geo_config)
        print("importing to postgis")
        print(geopandas_params)
    geo_file.to_postgis(geo_config["name"], conn, **geopandas_params)

#the "files" must be sorted, the idea
#is first be containers folders
#then the contained ones with their files
def get_closer_file(files, one):
    dirs = os.path.split(one)[:-1]
    for i in reversed(range(len(dirs))):
        _file = os.path.join(*dirs[0:i], 'config.json')
        if _file in files:
            return _file
    return None

def get_config(ifolder):
    config = {}
    default = {}
    #This is tricky, in order to join configs, all of them will be sorted
    #because, in that way, every next folder, will be contained in a upper one
    #is easier to merge
    configs = glob.glob(os.path.join(ifolder, "**/config.json"), recursive=True)
    configs.sort()
    for config_file in configs:
        config[config_file] = {}
        with open(config_file) as json_file:
            data = json.load(json_file)
        for db in data['dbs']:
            data[db] = get_safe_engine(data[db])
        if 'default' in data:
            upper = get_closer_file(configs, config_file)
            if upper is None:
                default[config_file] = data['default']
            else:
                default[config_file] = join2configs(upper, data['default'])
        config[config_file] = data['dbs']
    return config

class Cache_json:
    def __init__(self, path):
        self.path = path
        self.schema = None
    def get(self):
        if self.schema is None:
            self.schema = fast_json(self.path)
        return self.schema

supported_extensions = {
    "shp": {
        "scheme_config": Cache_json(pathlib.Path(os.path.join(script_path, "schemas/shp_config_schema/schema.json"))),
        "export_f": pandas2sql
    }
}

def load_by_extensions(ifolder, config):
    for extension in supported_extensions:
        for file in glob.glob(os.path.join(ifolder, "**/*.{}".format(extension)), recursive=True):
            file = pathlib.Path(file)
            supported_extensions[extension]["export_f"](file, config)

def valid_db_config(ifolder):
    config_schema = None
    with open(os.path.join(script_path, "schemas/db_schema/schema.json")) as schema:
        config_schema = json.load(schema)
    for config_file in glob.glob(os.path.join(ifolder, "**/dbs.json"), recursive=True):
        data = None
        with open(config_file) as json_file:
            data = json.load(json_file)
        jsonschema.validate(data, config_schema)

def geo2sql(ifolder):
    valid_db_config(ifolder)
    config = get_config(ifolder)
    if __debug__:
        print("config")
        print(config)
    load_by_extensions(ifolder, config)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description = 'import geo files massively to SQL server')
    parser.add_argument('folder', help='input folder')
    args = parser.parse_args()
    if os.path.isdir(args.folder):
        geo2sql(args.folder)
    else:
        raise NameError("No input folder")
