# -*- coding: utf-8 -*-
"""

@author: Mario Krapp
"""
import json
import os
import pandas as pd
import pickle
import sys
import urllib.request

# https://stackoverflow.com/a/287944/1498309
HEADER = '\033[95m'
OKBLUE = '\033[94m'
OKCYAN = '\033[96m'
OKGREEN = '\033[92m'
WARNING = '\033[93m'
FAIL = '\033[91m'
ENDC = '\033[0m'
BOLD = '\033[1m'
UNDERLINE = '\033[4m'

class NOAAStudy:
    """NOAA Study Class
    This class creates objects to hold data and metadata from the NOAA Paleoclimatology website.

    Parameters
    ----------

    Attributes
    ----------

    """

    def __init__(self, id, dataset_id=-1, cache=False):
        self.id       = id
        self.ds_id    = dataset_id
        self.cache    = cache
        self.metadata = {}
        self.data     = pd.DataFrame()
        self.info     = []

        has_metadata = False
        if self.cache == True:
            has_metadata = self.metadata_from_pickle()
        if not has_metadata:        
            self.get_metadata()
        has_data = False
        if self.cache == True:
            has_data = self.data_from_pickle()
        if not has_data:        
            self.get_data(self.ds_id)

    def metadata_from_pickle(self, cachedir=''):
        """
        Loads a NOAA Paleoclimatology metadata object from a pickle file.

        Parameters
        ----------
        cachedir : str
            the name of the cache directory
        """

        home = os.path.expanduser("~")
        if cachedir=='':
            cachedir=home+'/'+'noaapaleopy_cache'

        ret = False
        fnm_pkl = f"{cachedir}/{self.id}.pkl"
        if os.path.isfile(fnm_pkl):
            print(f"{BOLD}Loading cached metadata for study {self.id}.{ENDC}")
            with open(fnm_pkl,'rb') as f:
                self.metadata = pickle.load(f)
            ret = True
        else:
            ret = False
        return ret

    def metadata_to_pickle(self, cachedir=''):
        """
        Save a NOAA Paleoclimatology object to a pickle file.

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

        fnm_pkl = f"{cachedir}//{self.id}.pkl"
        print(f"{BOLD}Caching metadata for study {self.id}{ENDC}")
        with open(fnm_pkl,'wb') as f:
            pickle.dump(self.metadata,f)


    def get_metadata(self, cachedir=''):
        """
        Get metadata from NOAA request.

        Parameters
        ----------
        cachedir : str
            the name of the cache directory
        """
        url_base = f"https://www.ncdc.noaa.gov/paleo-search/study/search.json?NOAAStudyId={self.id}"
        with urllib.request.urlopen(url_base) as f:
            self.metadata = json.loads(f.read())['study'][0]
        if self.cache == True:
            self.metadata_to_pickle()

    def data_from_pickle(self, cachedir=''):
        """
        Load data from NOAA Paleoclimatology study data from a pickle file.

        Parameters
        ----------
        cachedir : str
            the name of the cache directory
        """

        home = os.path.expanduser("~")
        if cachedir=='':
            cachedir=home+'/'+'noaapaleopy_cache'

        ret = False
        fnm_pkl = f"{cachedir}/{self.id}_{self.ds_id}.pkl"
        if os.path.isfile(fnm_pkl):
            print(f" {BOLD}Loading cached file has been cached for {self.id} [{self.ds_id}].{ENDC}")
            with open(fnm_pkl,'rb') as f:
                [self.info,self.data] = pickle.load(f)
            ret = True
        else:
            ret = False
        return ret

    def data_to_pickle(self, cachedir=''):
        """
        Save a NOAA Paleoclimatology dataset to a pickle file.

        Parameters
        ----------
        cachedir : str
            the name of the cache directory
        """

        home = os.path.expanduser("~")
        if cachedir=='':
            cachedir=home+'/'+'noaapaleopy_cache'

        fnm_pkl = f"{cachedir}/{self.id}_{self.ds_id}.pkl"

        print(f" {BOLD}Caching file for {self.id} [{self.ds_id}]{ENDC}")
        with open(fnm_pkl,'wb') as f:
            pickle.dump([self.info,self.data],f)

    def get_data(self,n=-1):
        """
        Get dataset from NOAA request.

        Parameters
        ----------
        n : int
            the number of the dataset in this study
        """

        if n == -1:
            c = 0
            info = []
            for i,s in enumerate(self.metadata['site']):
                siteName = s['siteName']
                for p in s['paleoData']:
                    for n,d in enumerate(p['dataFile']):
                        file_url = d['fileUrl']
                        print(f" {BOLD}[{c}]{ENDC}{OKBLUE} '{file_url}' ({siteName}){ENDC}")
                        info.append([s['siteName'],file_url])
                        c += 1
            if c>1:
                i = int(input(f"{BOLD}Found multiple entries. Select [0-{c-1}]: {ENDC}"))
            else:
                i = 0
            self.info = info[i]
            self.ds_id = i
        else:
            self.ds_id = n
        suffix = self.info[1].split(".")[-1]
        if suffix == "txt":
            self.data = pd.read_csv(self.info[1],sep="\t",comment="#",encoding_errors="ignore",na_values=-999.00)
        elif suffix == "csv":
            self.data = pd.read_csv(self.info[1],encoding_errors="ignore",na_values=-999.00)
        else:
            print(f"{FAIL}Filetype '{suffix}' is not supported.{ENDC}")
            sys.exit()
        if self.cache == True:
            if not self.data_from_pickle():
                self.data_to_pickle()
