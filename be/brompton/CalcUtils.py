from typing import List,Dict
import re

from typing import List,Union,Any,Dict,Tuple
from collections import namedtuple
from enum import Enum

columns = ['meas_id', 'calc_tag', 'input_label', 'comment', 'input_id', 'constant_number', 'constant_string',
           'input_datasource',
           'expression', 'template_id', 'input_tag', 'datasource','poll_period','data_type']


RowTuple=namedtuple('RowTuple',columns)

class ResponseError(Exception):
    def __init__(self, message):
        super().__init__(message)

def get_poll_in_minutes(period:str):
    if(period.endswith("min")):
        return int(period[:-3])
    elif(period.endswith("hr")):
        return 60*int(period[:-2])
    elif(period.endswith("day")):
        return 24*60*int(period[:-3])
    elif(period=='DAILY'):
        return 24*60
    else:
        return 7*24*60  # Weekly

def get_offline_calcs(hook)->Dict[str,Dict[int,List[Tuple[int,str]]]]:
    offline_cals_qry = f"""select coalesce(p.value,'5min') poll_period,cust_id,output_measurement,data_type
                            from calculation_instance i join v_measurements m on i.ispersisted=true and i.output_measurement=m.id
                            left join calculation_period p on p.id=i.poll_period"""
    calcs = {}  # {poll_minutes->{cust_id:[output measurements]}

    # connect to db
    cur = None
    try:
        conn = hook.get_conn()
        # Select id,a_type,m_type,location,data_type by type
        print(f"Reading offline calcs")
        cur = conn.cursor()
        cur.execute(offline_cals_qry)
        rows = cur.fetchall()
        for row in rows:
            poll_period=row[0]
            if(not poll_period in calcs):
                calcs[poll_period]={}
            if(not row[1] in calcs[poll_period]):
                calcs[poll_period][row[1]]=[]
            calcs[poll_period][row[1]].append((row[2],row[3]))
    except Exception as e:
        raise (e)
    finally:
        if (cur):
            cur.close()
    return calcs

def get_calcs_graph(hook,ids:List)->(Dict,Dict,List):
    # query for calculation metadata
    calc_nodes={}
    meas_nodes={}
    calc_nodes_remaining =[int(id) for id in ids]
    calc_layers=[]
    while(calc_nodes_remaining):
        # remove already read nodes from remaining
        if(calc_nodes_remaining):  # protect from empty list when only one layer exists
            calc_layers.append(calc_nodes_remaining)
        calc_nodes_to_read=[node for node in calc_nodes_remaining if not(node in calc_nodes)]
        if(calc_nodes_to_read):
            # connect to db
            id_filter=','.join([str(id) for id in calc_nodes_to_read])
            calc_nodes_remaining=[]
            offline_cals_qry = f"select {','.join(columns)} from v_calculations where meas_id in ({id_filter})"
            cur = None
            try:
                conn = hook.get_conn()
                # Select id,a_type,m_type,location,data_type by type
                cur = conn.cursor()
                cur.execute(offline_cals_qry)
                rows = cur.fetchall()
                for row_in in rows:
                    row=RowTuple(*row_in)
                    if(not row.meas_id in calc_nodes):
                        calc_nodes[row.meas_id]={'tag':row.calc_tag,'inputs':{}}
                    calc_nodes[row.meas_id]['inputs'][row.input_label.strip()]={'comment':row.comment,'id':row.input_id,
                                                  'constant':row.constant_number if not row.constant_number is None else row.constant_string,
                                                  'interpolate':not row.input_datasource is None}
                    if(not 'expression' in calc_nodes[row.meas_id]):
                        calc_nodes[row.meas_id]['expression']=row.expression
                        calc_nodes[row.meas_id]['template_id']=row.template_id
                        calc_nodes[row.meas_id]['poll_period']=row.poll_period
                        calc_nodes[row.meas_id]['data_type']=row.data_type
                    if(row.input_id and row.datasource and row.input_datasource==row.datasource):  # if input is calc
                        if(not row.input_id in calc_nodes_remaining):
                            calc_nodes_remaining.append(row.input_id)
                    elif(row.input_id):
                        if(not row.input_id in meas_nodes):
                            meas_nodes[row.input_id]={'tag':row.input_tag,"calcs":{}} # list of calcs that use it
                        meas_nodes[row.input_id]['calcs'].update({row.meas_id:row.input_label.strip()})
            except Exception as e:
                raise (e)
            finally:
                if (cur):
                    cur.close()
        else:
            break
    return calc_nodes,meas_nodes,calc_layers
