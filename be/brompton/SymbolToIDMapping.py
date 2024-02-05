from dataclasses import dataclass
from typing import List,Dict

@dataclass
class AssetMeasurement:
    assetid:int
    shorttag:str
    tag:str=None
    tsdbkey:int=None
    dtype:str=None

def get_asset_meas_from_db(asset_tags: Dict[int, List[str]], hook) -> Dict[int,Dict[str,AssetMeasurement]]:
        #List[AssetMeasurement]:
    asset_ids=set()
    short_tags=set()
    asset_short=[]
    for asset,shorttags in asset_tags.items():
        asset_ids.add(str(asset))
        for shorttag in shorttags:
            short_tags.add(shorttag)
            asset_short.append(f"{asset}:{shorttag}")
    asset_ids_str = ",".join(list(asset_ids))
    short_tags_str = "'"+"','".join(list(short_tags))+"'"
    asset_data_qry =f"""select a.id assetid,m.id tsdbkey,m.tag,d.name dtype,
                    case 
                        when position('\\' in m.tag)>0 then right(m.tag,position('\\' in reverse(m.tag))-1) 
                        else m.tag 
                    end shorttag
                from asset a 
                join asset_measurement am on 
                    am.asset=a.id and a.id in ({asset_ids_str}) 
                join measurement m on 
                    m.id=am.measurement and 
                    case 
                        when position('\\' in m.tag)>0 then right(m.tag,position('\\' in reverse(m.tag))-1) 
                        else m.tag 
                    end in ({short_tags_str})
                    and m.enabled=true
                join data_type d on m.data_type=d.id"""
    asset_meas={}
    # connect to db
    cur = None
    try:
        conn = hook.get_conn()
        # Select id,a_type,m_type,location,data_type by type
        print(f"Reading definitions for asset id's: {asset_ids_str}")
        cur = conn.cursor()
        cur.execute(asset_data_qry)
        rows = cur.fetchall()
        short_tags_not_missing={}
        for row in rows:
                entry=AssetMeasurement(assetid=row[0],tsdbkey=row[1],tag=row[2],dtype=row[3],shorttag=row[4])
                if(not row[0] in asset_meas):
                    asset_meas[row[0]]={}
                if(not row[4] in asset_meas[row[0]]):
                    asset_meas[row[0]][row[4]]=entry
                if(not row[0] in short_tags_not_missing):
                    short_tags_not_missing[row[0]]=[]
                short_tags_not_missing[row[0]].append(entry.shorttag)
    except Exception as e:
        raise(e)
    finally:
        if(cur):
            cur.close()
    # Mark missing items
    for asset in asset_tags.keys():
        asset_tags_expected=asset_tags[asset]
        for tag in asset_tags_expected:
            if (asset in short_tags_not_missing and tag in short_tags_not_missing[asset]):
                continue
            entry = AssetMeasurement(assetid=asset, shorttag=tag)
            if (not asset in asset_meas):
                asset_meas[asset] = {}
            if (not tag in asset_meas[asset]):
                asset_meas[asset][tag] = entry
    return asset_meas



# Read tag defs from db
def get_maps_from_db(file_pattern,assets,hook):
    asset_ids=[]
    for block in assets:
        if(not(block["asset"] in asset_ids)):  
            asset_ids.append(block["asset"])
    asset_ids_str=",".join(asset_ids)
    asset_data_qry=f"""select a.id asset,me.id measurement,m.column from 
	                    asset a 
                    join etl_file_column_map m on m.asset_type=a.a_type and a.id in ({asset_ids_str}) and file_pattern='{file_pattern}'
                    join asset_measurement am on am.asset=a.id
                    join measurement me on me.id=am.measurement and me.tag like '%' || m.tag and me.enabled=true"""

    maps = {} # {asset_id-->symbol-->meas_id}
    # connect to db
    cur = None
    try:
        conn = hook.get_conn()
        # Select id,a_type,m_type,location,data_type by type
        print(f"Reading definitions for asset id's: {asset_ids_str}")
        cur = conn.cursor()
        cur.execute(asset_data_qry)
        rows = cur.fetchall()
        for row in rows:
            if(not(row[0] in maps)):
                maps[row[0]]={}
            maps[row[0]][row[2]]=row[1]
    except Exception as e:
        raise(e)
    finally:
        if(cur):
            cur.close()
    return maps

