import biothings_client
import requests
import asyncio
from concurrent import futures
import requests

MAX_WORKERS = 5 # threadpoolExecutor max workers for clingen GET requests
MAX_GET = 9     # max id for clinget GET requests. if ids count exceeds this, then POST method will be used  


def get_client(biothing_type, instance=True, *args, **kwargs):
    if biothing_type == "variantwithregistry":
        return MyVariantWrapper()
    else:
        return biothings_client.get_client(biothing_type, instance, *args, **kwargs)

def extract_myvariant_id(json_res):
    """extract myvariant id from clingen response
    result : myvariant hg19 id list
    if not myvariant entry : return 0
    if error : return -1
    """
    try:
        _id = json_res['externalRecords']['MyVariantInfo_hg19'][0]['id']
    except KeyError:
        _id = None
        
    return _id

    
def get_myvariant_ids(hgvs_ids):
    workers = min(MAX_WORKERS, len(hgvs_ids))   
    
    with futures.ThreadPoolExecutor(workers) as executor:   
        res = executor.map(get_myvariant_id, hgvs_ids)   
  
    return list(res)

def post_myvariant_id(hgvs_ids):
    """clingen request with hgvs and get myvariant ids by POST method"""
    url = f'http://reg.genome.network/alleles?file=hgvs'
    res = requests.post(url, data="\n".join(hgvs_ids))
    return [ extract_myvariant_id(r) for r in res.json() ]


class MyVariantWrapper:
    def __init__(self):
        self._mv = get_client("variant")
        self.clingen_base_url = "http://reg.genome.network/allele"
        
    def getvariant(self, _id, fields=None, **kwargs):
        try:
            if kwargs["external"] == 'hgvsclingen':
                _id = self.convert_id(_id)
                del kwargs["external"]
                
        except KeyError:
            pass
        
        return self._mv.getvariant(_id, fields=fields, **kwargs)

    def getvariants(self, _ids, fields=None, **kwargs):
        """ if id count < 10 => fetch myvariant id using GET method """
        try:
            if kwargs["external"] == 'hgvsclingen':
                if len(_ids) > MAX_GET :
                    _ids = self.convert_ids(_ids, method='POST')
                else:
                     _ids = self.convert_ids(_ids, method='GET')                   
                del kwargs["external"]
                
        except KeyError:
            pass    
        
        return self._mv.getvariants(_ids, fields=fields, **kwargs)
        
    def convert_id(self, _id):
        return self._GET_convert_id(_id)
        
    def convert_ids(self, _ids, method='GET'):
        if method=='GET':
            return self._GET_convert_ids(_ids)
        
        elif method == 'POST':
            return self._POST_convert_ids(_ids)            
    

    def _extract_myvariant_id(self, json_res):
        try:
            _id = json_res['externalRecords']['MyVariantInfo_hg19'][0]['id']
        except KeyError:
            _id = None
        return _id 
    
    def _GET_convert_id(self, hgvs_id):
        url = f'http://reg.genome.network/allele?hgvs={hgvs_id}'    
        res = requests.get(url)
        return self._extract_myvariant_id(res.json())

    def _GET_convert_ids(self, hgvs_ids):
        workers = min(MAX_WORKERS, len(hgvs_ids))   

        with futures.ThreadPoolExecutor(workers) as executor:   
            res = executor.map(self._GET_convert_id, hgvs_ids)   

        return list(res)    
    
    def _POST_convert_ids(self, hgvs_ids):
        """clingen request with hgvs and get myvariant ids by POST method"""
        url = f'http://reg.genome.network/alleles?file=hgvs'
        res = requests.post(url, data="\n".join(hgvs_ids))
        return [ self._extract_myvariant_id(r) for r in res.json() ]