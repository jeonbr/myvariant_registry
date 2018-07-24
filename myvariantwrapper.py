import requests
import asyncio
from aiohttp import web
import functools

def extract_myvariant_id(json_res):
    """extract myvariant id from clingen response
    result : myvariant hg19 id list
    if not myvariant entry : return 0
    if error : return -1
    """
    _id = json_res['externalRecords']['MyVariantInfo_hg19'][0]['id']
    return _id

async def get_myvariant_id(hgvs):
    """clingen request with hgvs and get myvariant ids by GET method"""
    loop = asyncio.get_event_loop()
    url = f'http://reg.genome.network/allele?hgvs={hgvs}'
    fut = loop.run_in_executor(None, requests.get, url)
    res = await fut
    return extract_myvariant_id(res.json())

async def post_myvariant_id(hgvs_ids):
    """clingen request with hgvs and get myvariant ids by POST method"""
    loop = asyncio.get_event_loop()
    url = f'http://reg.genome.network/alleles?file=hgvs'
    fut = loop.run_in_executor(None, functools.partial(requests.post, data="\n".join(hgvs_ids)), url)
    res = await fut
    res = [ extract_myvariant_id(r) for r in res.json() ]
    return res

async def VariantGETHandler(request):
    variantid = request.match_info.get('variantid', False)
    args = dict(request.query)
    args_string = request.query_string
    loop = asyncio.get_event_loop()
    print(f"variantid: {variantid}")
    print(f"args: {dict(args)}")
    print(f"args_string: {args_string}")
    try:
        """ if GET has a external=hgvsclingen option
            Then get a hg19 myvariant id from clingen
            Else just send the GET request to Myvariant.info and
                return json response from Myvariant.info
        """ 

        if args['external'] == 'hgvsclingen':
            print("clingen hgvs request detected!")
            del args['external']
            _id = await get_myvariant_id(variantid)
            url = f"http://myvariant.info/v1/variant/{_id}"
            fut = loop.run_in_executor(None, 
                                       functools.partial(requests.get, params=args),
                                       url)
            res = await fut
            # res = requests.get(url, params=args ).json()
            return web.json_response(res.json()) 

        text= "\nOther external field options were detected!"
        return web.Response(text=text)

    except KeyError:
        url = f"http://myvariant.info/v1/variant/{variantid}?{args_string}"
        fut = loop.run_in_executor(None, 
                           functools.partial(requests.get, params=args),
                           url)
        res = await fut
        # res = requests.get(url).json()
        return web.json_response(res.json())    

async def VariantPOSTHandler(request):
    data = await request.post()
    data = dict(data)
    variantids = data.get('ids', False )
    loop = asyncio.get_event_loop()
    print(f"variantid: {variantids}")
    print(f"data: {dict(data)}")

    try:
        """ if POST has a external=hgvsclingen option
            Then get a hg19 myvariant id from clingen
            Else just send the GET request to Myvariant.info and
                return json response from Myvariant.info

            myvariant multiple id : ","
            clingen multiple id : new line 
        """ 

        if data['external'] == 'hgvsclingen':
            print("clingen hgvs request detected!")
            del data['external']
            _ids = await post_myvariant_id(variantids.split(","))
            data['ids'] = ",".join(_ids) 
            url = f"http://myvariant.info/v1/variant"
            fut = loop.run_in_executor(None, 
                                       functools.partial(requests.post, data=data),
                                       url)
            res = await fut
            return web.json_response(res.json()) 

        text= "\nOther external field options were detected!"
        return web.Response(text=text)

    except KeyError:
        url = f"http://myvariant.info/v1/variant"
        fut = loop.run_in_executor(None, 
                           functools.partial(requests.post, data=data),
                           url)
        res = await fut
        # res = requests.get(url).json()
        return web.json_response(res.json())  

if __name__ == "__main__":
    app = web.Application()
    app.add_routes([web.get('/variant/{variantid}', VariantGETHandler)])
    app.add_routes([web.post('/variant', VariantPOSTHandler)])
    web.run_app(app)   
