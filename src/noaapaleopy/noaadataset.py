# -*- coding: utf-8 -*-
"""

@author: Mario Krapp
"""
import io
import os
import json
import numpy as np
import pandas as pd
import pickle
import requests
import sys
from collections import Counter, OrderedDict
import logging

#LEVEL = logging.DEBUG
LEVEL = logging.INFO
# create logger
logger = logging.getLogger('NOAAPaleoPy')
logger.setLevel(LEVEL)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(LEVEL)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logging.basicConfig(filename='/tmp/noaapaleopy.log', level=LEVEL)


class NOAAEvent:
    """NOAA Event Class
    This class creates objects to hold event information from NOAA Paleoclimatology data sets.

    Parameters
    ----------

    Attributes
    ----------

    """
    def __init__(self, label, lon, lat):
        self.logger = logging.getLogger('NOAAPaleoPy.Event')
        self.label  = label
        self.lat    = lat
        self.lon    = lon
        self.logger.info(f"New Event '{self.label}'")

class NOAAParam:
    """ NOAA Parameter
    Shoud be used to create NOAA parameter objects. Parameter is used here to represent 'measured variables'
    
    Parameters
    ----------
    name : str
        A name to identify the parameter
    long_name : str
        The corresponding long name for the parameter
    unit : str
        the unit of measurement used with this parameter (e.g. m/s, kg etc..)
    
    Attributes
    ----------
    id : int
        the identifier for the parameter
    name : str
        A name to identify the parameter
    col_name : str
        The corresponding long name for the parameter
    unit : str
        the unit of measurement used with this parameter (e.g. m/s, kg etc..)
    
    """
    def __init__(self, name, long_name, unit=None):
        self.logger = logging.getLogger('NOAAPaleoPy.Param')
        ages = {
                "calendar kiloyear before present": "ka BP",
                "calendar kiloyears before present": "ka BP",
                "cal kyr BP": "ka BP",
                "calendar kyears before present": "ka BP",
                "Age_kyr": "ka BP",
                "cal ka BP": "ka BP",
                "calendar ka before 1950AD": "ka BP",
                "calendar kiloyears before 1950 AD": "ka BP",
                "calendar kiloyears before 1950": "ka BP",
                "calendar Kyears before present": "ka BP",
                "kyr BP": "ka BP",
                "kyrs": "ka BP",
                "calendar ka before present": "ka BP",
                "cal kiloyears before present": "ka BP"
                }
        self.name      = name
        self.long_name = long_name
        self.unit      = unit
        if self.unit in ages.keys():
            self.unit = ages[self.unit]
        self.logger.info(f"New Param '{self.name}' [{self.unit}]")

class NOAATable:
    """NOAA Table Class
    This class creates objects to hold data from NOAA Paleoclimatology tabular data.

    Parameters
    ----------

    Attributes
    ----------

    """

    def __init__(self,params,data,event):
        self.logger = logging.getLogger('NOAAPaleoPy.Table')
        self.params = params
        self.data   = data
        self.event  = event

    @classmethod
    def from_txtfile(cls,file_url,event):
        logger = logging.getLogger('NOAAPaleoPy.Table.from_txtfile')
        components = ["what", "material", "error", "units", "seasonality", "archive", "detail", "method", "format"]
        params = []
        skiprows = 0
        intermediate = 0
        with open(file_url,"r", encoding="utf-8", errors="ignore") as f:
            all_lines = f.read().splitlines()
            # store lines with parameters
            parameter_lines = []
            for l in all_lines:
                if l.startswith("##"):
                    parameter_lines.append(l.split("##")[-1])
                if l.startswith("#"):
                    skiprows = skiprows + 1 + intermediate
                    intermediate = 0
                else:
                    intermediate += 1
            logger.debug(f"skiprows = {skiprows}")

            # parse order of params from ## lines:
            for line in parameter_lines:
                p = line.split()[0]
                l = line.replace(p,"")
                p = p.strip()
                this_param = {"name": p}
                for i,c in enumerate(components):
                    x = l.split(",")
                    if i < len(x):
                        this_param[c] = x[i].strip() 
                        logger.debug(f"{p} - {c}: {x[i].strip()}")
                params.append(this_param)

            # read tabular data
            na_values = [-999.00,-9999.00]
            columns = [p["name"] for p in params]
            # https://stackoverflow.com/a/56454407/1498309
            d = {a:[i if i > 0 else "" for i in range(b)] if b>1 else '' for a,b in Counter(columns).items()}
            columns = [i+str(d[i].pop(0)) if len(d[i]) else i for i in columns]
            for i,p in enumerate(params):
                p["name"] = columns[i]
            logger.debug(f"columns: {columns} ({len(columns)})")
            logger.info(f"params: {columns}")
            usecols = list(range(len(columns)))
            # create StringIO object to pass on to Pandas
            s = io.StringIO("\n".join(all_lines))
            df = pd.read_csv(s,index_col=False,sep="\t",encoding_errors="ignore",
                    on_bad_lines='skip',na_values=na_values,
                    usecols=usecols,skiprows=skiprows)
            # assign columns names (may be different from actual table column names)
            df.columns = columns
            # add event-specific information
            df["Event"]     = event.label
            df["Latitude"]  = event.lat
            df["Longitude"] = event.lon
            #logger.debug(df)
            logger.debug("START Table info:\n"+df.describe().loc[["min","max"]].to_string())
            logger.debug("END Table info")
            # store parameters
            noaa_params = OrderedDict()
            for p in params:
                if "units" in p.keys():
                    unit = p["units"]
                else:
                    unit = ""
                noaa_params[p["name"]] = NOAAParam(name=p["name"],long_name=p["what"],unit=unit)
            
        return cls(noaa_params,df,event)

    @staticmethod
    def download(url, fnm):
        logger = logging.getLogger('NOAAPaleoPy.Table.download')
        logger.info(f"Download '{url}' to '{fnm}'")
        # open in binary mode
        with open(fnm, "wb") as file:
            # get request
            response = requests.get(url)
            # write to file
            file.write(response.content)

class NOAADataSet:
    """NOAA DataSet Class
    This class creates objects to hold data and metadata from the NOAA Paleoclimatology website.

    Parameters
    ----------

    Attributes
    ----------

    """

    def __init__(self, id, cache=True, metadata={}, data=pd.DataFrame(), params=dict(), title=None, events=[]):
        self.logger = logging.getLogger('NOAAPaleoPy.DataSet')
        self.cache      = cache
        self.metadata   = metadata
        # attributes according to PanDatSet
        self.id         = id
        self.events     = events
        self.title      = title
        self.params     = params
        self.data       = data
        self.logger.info(f"New DataSet '{self.id}'")
        # read metadata
        if self.cache == True:
            self.metadata = self.metadata_from_json()
        if not self.metadata:        
            self.metadata = self.get_metadata()
        self.link = self.metadata['onlineResourceLink']
        self.doi  = self.metadata['doi']
        self.title = self.metadata['studyName']
        self.logger.info(f"StudyID: {self.id}")
        self.logger.info(f"Title: {self.title}")
        self.logger.info(f"DOI: {self.doi}")
        has_data        = False

        # iteate over events (sites)
        if not has_data:
            try:
                self.get_data()
            except Exception as e:
                self.logger.error(f" {self.id}: Couldn't process {self.title}", exc_info=e)
        self.logger.debug("START DataSet info:\n"+self.data.head().to_string())
        self.logger.debug("END DataSet info")

    def get_data(self):
        home = os.path.expanduser("~")
        cachedir=home+'/'+'noaapaleopy_cache'
        self.data = pd.DataFrame()
        self.params = OrderedDict()
        self.events = []
        for i,site in enumerate(self.metadata['site']):
            siteName = site['siteName']
            lat = site["geo"]["geometry"]["coordinates"][0]
            lon = site["geo"]["geometry"]["coordinates"][1]
            event = NOAAEvent(label=siteName,lon=lon,lat=lat)
            self.events.append(event)
            #self.events.append(event)
            for table in site['paleoData']:
                for n,d in enumerate(table['dataFile']):
                    file_url = d['fileUrl']
                    self.logger.info(f"File: {file_url}.")
                    # Download files locally
                    table_dir = cachedir+"/"+str(self.id)
                    if not os.path.exists(table_dir):
                        os.makedirs(table_dir)
                    local_file = table_dir+"/"+file_url.split("/")[-1]
                    if not os.path.exists(local_file):
                        NOAATable.download(file_url,local_file)
                    if os.path.isdir(local_file):
                        logger.warning("File is a directory (skipping)!")
                    file_url = local_file
                    if file_url.split(".")[-1] == "txt":
                        t = NOAATable.from_txtfile(file_url,event)
                        self.data = pd.concat([self.data,t.data],axis=0,ignore_index=True)
                        self.params.update(t.params)


#    def save_data(self,fnm):
#        self.data.to_csv(fnm)
#
#    def load_data(self,fnm):
#        self.data = pd.read_csv(fnm,index_col=0)

#    @classmethod
#    def from_tables(cls,id,tables):
#        logger = logging.getLogger('NOAAPaleoPy.DataSet.from_tables')
#        data = pd.DataFrame()
#        params = OrderedDict()
#        for t in tables:
#            data = pd.concat([data,t.data],axis=0,ignore_index=True)
#            params.update(t.params)
#        return cls(id,data=data,params=params)

    def metadata_from_json(self, cachedir=''):
        """
        Loads a NOAA Paleoclimatology metadata object from a json file.

        Parameters
        ----------
        cachedir : str
            the name of the cache directory
        """

        home = os.path.expanduser("~")
        if cachedir=='':
            cachedir=home+'/'+'noaapaleopy_cache'

        fnm_json = f"{cachedir}/{self.id}.json"
        metadata = {}
        if os.path.isfile(fnm_json):
            self.logger.info(f"Loading cached metadata for study {self.id}.")
            with open(fnm_json,'r') as f:
                metadata = json.load(f)
        else:
            self.logger.warning(f"Loading cached metadata from '{fnm_json}' failed ({self.id}).")
        return metadata

    def metadata_to_json(self, metadata, cachedir=''):
        """
        Save a NOAA Paleoclimatology object to a json file.

        Parameters
        ---
        cachedir : str
            the name of the cache directory
        """
        home = os.path.expanduser("~")
        if cachedir=='':
            cachedir=home+'/'+'noaapaleopy_cache'
        if not os.path.exists(cachedir):
            os.makedirs(cachedir)

        fnm_json = f"{cachedir}/{self.id}.json"
        self.logger.info(f"Caching metadata for study {self.id}.")
        with open(fnm_json,'w') as f:
            json.dump(metadata,f)


    def get_metadata(self, cachedir=''):
        """
        Get metadata from NOAA request.

        Parameters
        ----------
        cachedir : str
            the name of the cache directory
        """
        url_base = f"https://www.ncdc.noaa.gov/paleo-search/study/search.json?NOAAStudyId={self.id}"
        self.logger.info(f"Get metdata from '{url_base}'")
        response = requests.get(url_base).json()
        metadata = response['study'][0]
        if self.cache == True:
            self.metadata_to_json(metadata)
        return  metadata
