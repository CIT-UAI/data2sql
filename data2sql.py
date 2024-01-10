import geopandas
import glob
import os
import os.path
import json
import sqlalchemy
import jsonschema
import pathlib
import re
import osgeo.gdal

import tempfile

script_path = os.path.dirname(os.path.realpath(__file__))

def get_safe_engine(conn):
    return conn
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

def join2configs(container, contained):
    if container is not None:
        container = container.copy()
    if contained is not None:
        contained = contained.copy()
    if container is None:
        return contained
    if contained is None:
        return container
    if 'mix' in contained and contained['mix'] == 'clean':
        return contained
    if ('mix' in contained and contained['mix'] == 'replace') or \
        'mix' not in contained:
        ret = container.copy()
        if 'mix' in ret:
            del ret['mix']
        for i in contained:
            ret[i] = contained[i]
        return ret
    raise NameError("Not supported way to join configs: {}".format(contained['mix']))

def get_file_config(geo_file_path, config):
    file_config = geo_file_path.with_suffix(".json")
    if os.path.exists(file_config): 
        file_config = fast_json(geo_file_path.with_suffix(".json"))
    else:
        file_config = None
    g_default, g_config = get_closer_config(config, geo_file_path)
    file_config = join2configs(g_default, file_config)
    if g_config is None or file_config['db'] not in g_config:
        raise NameError("The DB {} was not found".format(file_config['db']))
    return g_config[file_config['db']], file_config

def pandas2sql(geo_file_path, config, arcgis=True):
    output = tempfile.NamedTemporaryFile(suffix=".gpkg")
    if __debug__:
        print("Reading {}".format(geo_file_path))
    try:
        geo_file = geopandas.read_file(geo_file_path)
        _ = geo_file.geometry.name
        if arcgis:
            geo_file.columns = geo_file.columns.str.lower()
        geo_file = geo_file.set_geometry(_.lower())
    except Exception as e:
        print(e)
        raise NameError("The file {}\n can't be loaded".format(geo_file_path))
    conn, geo_config = get_file_config(geo_file_path, config)
    if 'name' not in geo_config or 'db' not in geo_config:
        return
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
    if ('optional_index' in geo_config) and 'index_label' not in geopandas_params:
        for opt in geo_config['optional_index']:
            if opt in geo_file.columns:
                geopandas_params['index'] = False
                geopandas_params['index_label'] = opt
                break
    if __debug__:
        print("geo file config {}".format(geo_file_path))
        print(geo_config)
        print("importing to sql")
        print(geopandas_params)
        print(geo_file.columns)
    os.remove(output.name)
    geo_file.to_file(output.name, layer = geo_config["name"].lower()) 
    #options = {
    #  
    #}
    osgeo.gdal.VectorTranslate(
        conn,
        output.name,
        options = ''
    )
    #geo_file.to_postgis(geo_config["name"].lower(), conn, **geopandas_params)
    if __debug__:
        print("End reading {}\n".format(geo_file_path))

#the "files" must be sorted, the idea
#is first be containers folders
#then the contained ones with their files
def get_closer_config(files, one):
    dirs = one.parts[0:-1]
    for i in reversed(range(len(dirs))):
        _file = pathlib.Path(os.path.join(*dirs[0:i], 'config.json'))
        if _file in files:
            _ = files[_file]
            return _['default'], _['dbs']
    return None, None

def get_config(ifolder):
    config = {}
    #This is tricky, in order to join configs, all of them will be sorted
    #because, in that way, every next folder, will be contained in a upper one
    #is easier to merge
    configs = glob.glob(os.path.join(ifolder, "**/config.json"), recursive=True)
    configs.sort()
    configs = list(map(pathlib.Path, configs))
    for config_file in configs:
        config[config_file] = {'dbs': {}, 'default': {}}
        with open(config_file) as json_file:
            data = json.load(json_file)
        for db in data['dbs']:
            data['dbs'][db] = get_safe_engine(data['dbs'][db])
        upper_default, upper_dbs = get_closer_config(config, config_file)
        if 'default' in data:
            if upper_default is None:
                config[config_file]['default'] = data['default']
            else:
                config[config_file]['default'] = join2configs(upper_default, data['default'])
        if upper_dbs is not None:
            config[config_file]['dbs'] = upper_dbs
        for i in data['dbs']:
            config[config_file]['dbs'][i] = data['dbs'][i]
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

import hashlib

def hash_file(file):
    BUF_SIZE = 65536
    sha256 = hashlib.sha256()
    with open(file, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha256.update(data)
    return sha256.hexdigest()

def need_update(file, config):
    status = file.with_suffix("status")
    if not os.path.exists(status):
        return False
    

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
