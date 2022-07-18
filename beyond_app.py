# Core pkgs
#%% pip freeze > requirements. txt
import streamlit as st

## EDA Pkgs
import base64
import json
import pandas as pd
import os
import sys

##############
from logging.handlers import RotatingFileHandler
from elasticsearch import Elasticsearch
# from elasticsearch import logger as es_logger
from jinja2 import FileSystemLoader, Environment
import requests
from tqdm import tqdm
from unidecode import unidecode





@st.cache()
def load_es_data(es_client, es_index_pattern):
    df = ed.DataFrame(es_client,
                  es_index_pattern)
    return df

def load_es_data_localy(local_filename):
    df = pd.read_csv(local_filename, sep= '\t')
    return df


## list to for auto completion 
def load_list_from_txt(txtfile):
    els = []
    with open(txtfile, 'r') as fp:
        for line in fp:
            x = line[:-1]
            els.append(x)
    return els


# load lists and keep them in cache mode
# @st.cache()
def retrive_list():
    mi_list = load_list_from_txt("lists/mi_list.txt")
    mi_list.insert(0,'<select>')
    hb_list = load_list_from_txt("lists/hb_list.txt")
    hb_list.insert(0,'<select>')
    ph_list = load_list_from_txt("lists/ph_list.txt")
    ph_list.insert(0,'<select>')
    use_list = load_list_from_txt("lists/use_list.txt")
    use_list.insert(0,'<select>')
    return mi_list, hb_list, ph_list, use_list


def get_table_download_link_csv(df):
    """
    fnx to dowload the query results in csv file format
    """
    csv = df.to_csv(index=False, sep="\t").encode()
    b64 = base64.b64encode(csv).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="data.csv" target="_blank">Download csv file</a>'
    return href    


# from @Rémy
def elastic_pagination_scrolling(result, headers):
    """
    Elasticsearch limit results of query at 10 000. To avoid this limit, we need to paginate results and scroll
    This method append all pages form scroll search
    :param result: a result of a ElasticSearcg query
    :return:
    """
    scroll_size_total = result['hits']['total']["value"]
    print("scroll_size_total :", scroll_size_total)
    # Progress bar
    pbar = tqdm(total=scroll_size_total)
    results = []
    results += result['hits']['hits'] # we add the first pages of results before scrolling
    print("first len of results :", len(results))
    scroll_size = scroll_size_total#len(results)
    pbar.update(scroll_size)
    while (scroll_size > 0):
        try:
            scroll_id = result['_scroll_id']
            print("scroll_id", scroll_id)
            # res = client.scroll(scroll_id=scroll_id, scroll='60s')
            query = {
                "scroll": "1m",
                "scroll_id": scroll_id
            }
            query_json = json.dumps(query)
            print("query_json : ", query_json)
            res = requests.get(es_url + "_search/scroll",
                               data=query_json,
                               headers=headers,
                               ).json()
            
            print("interm len of res :", len(res))
            
            results += res['hits']['hits']

            scroll_size = len(res['hits']['hits'])
            pbar.update(scroll_size)
        except:
            pbar.close()
            break
    pbar.close()
    print("results list size", len(results))
    return results


@st.cache()
def GetMicroNCBI(jinja_env, es_url, index_es, list_of_micro_name):
    """
    Query elastic with selected fields
    :param es_url:
    :param index_es:
    :param list_of_micro_name:
    :return: corresponding taxid, and a dataframe
    
    """
    headers = {'content-type': 'application/json'}
    template = jinja_env.get_template("get_micro_ncbi_by_micro_name.json.j2")
    query = template.render(list_of_keywords=list_of_micro_name)
    """global r
    r = requests.get(es_url + index_es + "/_search?scroll=1m&size=10000",  # context for scrolling up to 1 minute
                         data=query,
                         headers=headers,
                         )"""
    try:
        r = requests.get(es_url + index_es + "/_search?scroll=1m&size=10000",  # context for scrolling up to 1 minute
                         data=query,
                         headers=headers,
                         )
    except Exception as e:
        print("Can not query: "+str(query))

    results = elastic_pagination_scrolling(r.json(), headers)
    
    df_results = pd.DataFrame(results)
    df_results = pd.json_normalize(df_results['fields'])
    # for every cell we retrieve the first value from list. Sometimes there is NaN value when there is empty value.
    df_results = df_results.applymap(lambda x: x[0] if isinstance(x, list) else '')
    taxid = df_results["document.taxid"].unique()#[0]
    #print(len(df_results))
    return taxid , df_results



def avoid10kquerylimitation(result, client):
    """
    Elasticsearch limit results of query at 10 000. To avoid this limit, we need to paginate results and scroll
    This method append all pages form scroll search
    :param result: a result of a ElasticSearcg query
    :return:
    """
    scroll_size = result['hits']['total']["value"]
#     print("scroll_size : ", scroll_size)
    results = []
    # Progress bar
    pbar = tqdm(total=scroll_size)
    while (scroll_size > 0):
#         try:
        scroll_id = result['_scroll_id']
        res = client.scroll(scroll_id=scroll_id, scroll='60s')
        results += res['hits']['hits']
        scroll_size = len(res['hits']['hits'])
        pbar.update(scroll_size)
#         except:
        pbar.close()
#         logger.error("elasticsearch search scroll failed")
#             break
    pbar.close()
    return results


def GetMicroDscHbt_only(jinja_env, es_url, index_es, client_es, list_of_micro_taxid):#, nb_of_estimated_results=10000):
    """
    Retrieves all tweets for a specific disease thanks its keywords
    :param jinja_env:
    :param es_url:
    :param index_es:
    :param list_of_keywords:
    :param disease:
    :param nb_of_estimated_results:
    :return:
    """
    template = jinja_env.get_template("get_micro_ncbi_by_micro_taxid.json.j2")
    #template = jinja_env.get_template("template.json.j2")
    query = template.render(list_of_keywords=list_of_micro_taxid)
#     print("desc", query)
    headers = {'content-type': 'application/json'}
    
    try:
        r = requests.get(es_url + index_es + "/_search?scroll=1m&size=10000",  # context for scrolling up to 1 minute
                         data=query,
                         headers=headers,
                         )
    except Exception as e:
        return -1
    
    results = avoid10kquerylimitation(r.json(), client_es)
    
#     print("avoid10kquerylimitation", len(results))

    df_results = pd.DataFrame(results)

    df_results = pd.json_normalize(df_results['fields'])
    # for every cell we retrieve the first value from list. Sometimes there is NaN value when there is empty value.
    df_results = df_results.applymap(lambda x: x[0] if isinstance(x, list) else '')

    return df_results

# @st.cache()
def GetHabitObtid(jinja_env, es_url, index_es, list_of_habit_name):
    """
    Query elastic with selected fields
    :param es_url:
    :param index_es:
    :param query:
    :return: a dataframe
    """
    headers = {'content-type': 'application/json'}
    template = jinja_env.get_template("get_habit_obtid_by_hbt_name.json.j2")
    #template = jinja_env.get_template("template.json.j2")
    query = template.render(list_of_keywords=list_of_habit_name)
    
    try:
        r = requests.get(es_url + index_es + "/_search?scroll=1m&size=10000",  # context for scrolling up to 1 minute
                         data=query,
                         headers=headers,
                         )
    except Exception as e:
        print("Can not query: "+str(query))
    
    df_results = pd.DataFrame(r.json()["hits"]["hits"])

    df_results = pd.json_normalize(df_results['fields'])
    # for every cell we retrieve the first value from list. Sometimes there is NaN value when there is empty value.
    df_results = df_results.applymap(lambda x: x[0] if isinstance(x, list) else '')
    obtid = df_results['document.obtid_habitat'].unique()#[0]
    print("obtid :", obtid)
#     print("df_results :" ,len(df_results))
    return obtid , df_results


# @st.cache()
def GetAllMicroWithObtidInTheirPath(jinja_env, es_url, index_es, client_es, list_of_obtid):
    """
    Query elastic with selected fields
    :param es_url:
    :param index_es:
    :param query:
    :return: a dataframe
    """
    headers = {'content-type': 'application/json'}
    template = jinja_env.get_template("get_doc_content_by_obtid.json.j2")
    #template = jinja_env.get_template("template.json.j2")
    query = template.render(list_of_keywords=list_of_obtid)
#     print(query)
    try:
        r = requests.get(es_url + index_es + "/_search?scroll=1m&size=10000",  # context for scrolling up to 1 minute
                         data=query,
                         headers=headers,
                         )
    except Exception as e:
        print("Can not query: "+str(query))
    
#     print("r.json()__ :", len(r.json()["_scroll_id"]))
    
    results = avoid10kquerylimitation(r.json(), client_es)

    df_results = pd.DataFrame(results)
    df_results = pd.json_normalize(df_results['fields'])
    # for every cell we retrieve the first value from list. Sometimes there is NaN value when there is empty value.
    df_results = df_results.applymap(lambda x: x[0] if isinstance(x, list) else '')

    print("whole df_results :" ,len(df_results))
    return df_results

#@st.cache()
def GetMicroDscHbt(jinja_env, es_url, index_es, client_es, list_of_micro_taxid):
    """
    Retrieves all tweets for a specific disease thanks its keywords
    :param jinja_env:
    :param es_url:
    :param index_es:
    :param list_of_keywords:
    :param disease:
    :param nb_of_estimated_results:
    :return:
    """
    template = jinja_env.get_template("get_micro_ncbi_by_micro_taxid.json.j2")
    #template = jinja_env.get_template("template.json.j2")
    query = template.render(list_of_keywords=list_of_micro_taxid)
    headers = {'content-type': 'application/json'}

    try:
        r = requests.get(es_url + index_es + "/_search?scroll=1m&size=10000",  # context for scrolling up to 1 minute
                         data=query,
                         headers=headers,
                         )
    except Exception as e:
        print("Can not query: "+str(query))
    
    results = avoid10kquerylimitation(r.json(), client_es)
#     results = elastic_pagination_scrolling(r.json(), headers)
    
#     print("GetMicroDscHbt max len", len(results))
    
    df_results = pd.DataFrame(results)
    df_results = pd.json_normalize(df_results['fields'])
    # for every cell we retrieve the first value from list. Sometimes there is NaN value when there is empty value.
    df_results = df_results.applymap(lambda x: x[0] if isinstance(x, list) else '')

    list_of_micro_and_descd_habits = df_results["document.form_habitat"].unique()
    list_of_micro_and_descd_habits = [i for i  in list_of_micro_and_descd_habits if i] 

    #taxid = df_results["document.taxid"].unique()[0]

    l_path = df_results["document.paths_microorganism"].unique()
    #tmp = l[1].split('/')
    l_path = [i for i in l_path if i]
    desclist = []
    ascdlist = []
    for p in range(len(l_path)):
        tmp = l_path[p].split('/')
        desc = tmp[tmp.index(list_of_micro_taxid[0]):]
        desclist.extend(desc)
        asc = tmp[:tmp.index(list_of_micro_taxid[0])]
        ascdlist.extend(asc)
    ascdlist = [a for a in ascdlist if a]
    desclist = [d for d in desclist if d]
    
    return list(set(desclist)), list(set(ascdlist)), df_results, list_of_micro_and_descd_habits

# @st.cache()
def GetDfWithListOfHabitat(jinja_env, es_url, index_es, client_es, list_of_micro_and_descd_habits):
    """
    Query elastic with selected fields
    :param es_url:
    :param index_es:
    :param query:
    :return: a dataframe
    """
    headers = {'content-type': 'application/json'}
    template = jinja_env.get_template("get_habit_obtid_by_hbt_name.json.j2")
    #template = jinja_env.get_template("template.json.j2")
    query = template.render(list_of_keywords=list_of_micro_and_descd_habits)
    
    try:
        resp = requests.get(es_url + index_es + "/_search?scroll=1m&size=10000",  # context for scrolling up to 1 minute
                         data=query,
                         headers=headers,
                         )
    except Exception as e:
        return -1

    results = avoid10kquerylimitation(resp.json(), client_es)
    df_results = pd.DataFrame(results)
#     print("len of df : ", len(df_results))
    
    df_results = pd.json_normalize(df_results['fields'])  
    
#     # for every cell we retrieve the first value from list. Sometimes there is NaN value when there is empty value.
    df_results = df_results.applymap(lambda x: x[0] if isinstance(x, list) else '')
    print(df_results)
#     print("df_results :" ,len(df_results))
    df_results = df_results.drop_duplicates()
    return  df_results

def main():
    """
    main Fxn, we build the UI with Streamlit (st) 
    """    

    COLSTOKEEP = ['document.pmid', 'document.taxid',
       'document.canonical_habitat', 'document.canonical_microorganism',
       'document.form_habitat', 'document.form_microorganism',
       'document.lemma_microorganism', 'document.obtid_habitat',
       'document.offset_habitat', 'document.offset_microorganism',
       'document.paths_habitat', 'document.paths_microorganism',
       'document.section']
    path_figs_dir = os.path.join(os.path.dirname(__file__), "figs")
    # Init Elasticsearch configurations
    es_url="http://cin-mo-rodrique_catalogue.montpellier.irstea.priv:9200/"
    client_es = Elasticsearch(es_url)
    index_es="beyond_relations_full"
    # init jinja2 configuration
    template_dir = os.path.join(os.path.dirname(__file__), "eda_templates")
    jinja_env = Environment(loader=FileSystemLoader(template_dir))


    mi_list, hb_list, ph_list, use_list = retrive_list()

    st.title('Beyond Dashboard')

    menu = ["Beyond demo"] 
    choice = st.sidebar.selectbox("Menu", menu)   
    if choice == "Beyond demo": # we are using Demo Data menu in this section

        st.subheader("Paramètres de la Recherche")
        all_micro,micro,all_habit,habitat = st.columns([1,2,1,2])

        Mi_DESC,Hb_DESC = st.columns([1,1])

        all_micro = all_micro.checkbox("all micro")
        micro_option = micro.selectbox('Microorganism', mi_list)
        Mi_DESC = Mi_DESC.checkbox("Mi_DESC")
#         Mi_ASCD = Mi_ASCD.checkbox("Mi_ASCD")


        all_habit = all_habit.checkbox("all habit")
        habitat_option = habitat.selectbox('Habitat', hb_list)
        Hb_DESC = Hb_DESC.checkbox("Hb_DESC")
#         Hb_ASCD = Hb_ASCD.checkbox("Hb_ASCD")

        divider = st.columns([5])

        Scrap_button = st.button("Start Retriving") 


        if Scrap_button:
            if (micro_option != '<select>' and habitat_option =='<select>'  and all_habit and Mi_DESC and not (Hb_DESC or all_micro)):
                st.write('Looking for all living places for :', micro_option , " and descendants")

                list_of_micro_name = [micro_option]
#                 list_of_micro_taxid = GetMicroNCBI(jinja_env, es_url, index_es, list_of_micro_name)
                list_of_micro_taxid, df_results = GetMicroNCBI(jinja_env, es_url, index_es, list_of_micro_name)
                
                st.write("Corresponding Taxid : ",list_of_micro_taxid[0])
#                 st.write(repr(len(df_results)) + '  documents dans le corpus')

                df_micro = GetMicroDscHbt_only(jinja_env, 
                                               es_url, 
                                               index_es, 
                                               client_es, 
                                               list_of_micro_taxid)

                df_micro = df_micro[COLSTOKEEP]
                df_micro = df_micro.drop_duplicates()

                st.dataframe(df_micro.head(10))
                st.write(repr(len(df_micro)) + '  documents dans le corpus')
                st.write('Size of query corpus : ', repr(round(sys.getsizeof(df_micro)/1000000,2))+' '+'Mb')
                PMID = len(list(set(df_micro['document.pmid'].tolist())))
                hbt = len(list(set(df_micro['document.form_habitat'].tolist())))
                st.write("Unique PMID : ", PMID)
                st.write("Unique HABITAT : ", hbt)
                st.write("Unique PMID - First@10",
                         [i for i in list(set(df_micro['document.pmid'].tolist()))[:10] if i])
                st.write("Unique HABITAT - First@10",
                         [i for i in list(set(df_micro['document.form_habitat'].tolist()))[:10] if i])


#                 st.markdown(get_table_download_link_csv(df_micro), unsafe_allow_html=True)
                
                

            elif habitat_option != '<select>' and micro_option =='<select>'  and all_micro and Hb_DESC and not (Mi_DESC or all_habit) :
                st.write('Looking for all micoorganism that live in  :', habitat_option)

                list_of_habit_name = [habitat_option]

                obtid, df_results = GetHabitObtid(jinja_env, 
                                                  es_url, 
                                                  index_es, 
                                                  list_of_habit_name)
    
                st.write("Corresponding Obtid : ",obtid[0])
#                 st.write(repr(len(df_results)) + '  documents dans le corpus')

                micro_df = GetAllMicroWithObtidInTheirPath(jinja_env, es_url, index_es, client_es, obtid)

                micro_df = micro_df[COLSTOKEEP]
#                 micro_df = micro_df.astype(str)
#                 #micro_df = micro_df.dtypes.astype(str)
                micro_df = micro_df.drop_duplicates()
                st.dataframe(micro_df.head(10))

                st.write(repr(len(micro_df)) + '  documents dans le corpus')
                st.write('Size of query corpus : ', repr(round(sys.getsizeof(micro_df)/1000000,2))+ ' '+'Mb')
                PMID = len(list(set(micro_df['document.pmid'].tolist())))
                TAXON = len(list(set(micro_df['document.form_microorganism'].tolist())))
                st.write("Unique PMID : ", PMID)
                st.write("Unique HABITAT : ", TAXON)
                st.write("Unique PMID - First@10", 
                         [i for i in list(set(micro_df['document.pmid'].tolist()))[:10] if i])
                st.write("Unique TAXON - First@10",
                         [i for i in list(set(micro_df['document.form_microorganism'].tolist()))[:10] if i])
#                 st.markdown(get_table_download_link_csv(micro_df), unsafe_allow_html=True)

                
                
            elif habitat_option != '<select>' and micro_option !='<select>' and Mi_DESC and Hb_DESC and not (all_habit or all_micro):
                st.write('Looking for ', micro_option, ' that lives in ', habitat_option, " with all descendants")
                
                list_of_micro_name = [micro_option]
                list_of_micro_taxid, df_results = GetMicroNCBI(jinja_env, es_url, index_es, list_of_micro_name)
                st.write("Corresponding Taxid : ",list_of_micro_taxid[0])    
            
            
                
                desclist, ascdlist, habit_df, list_of_micro_and_descd_habits = GetMicroDscHbt(jinja_env, 
                                                                                              es_url, 
                                                                                              index_es,
                                                                                              client_es,
                                                                                              list_of_micro_taxid)
                habit_df = habit_df[COLSTOKEEP]
#                 habit_df = habit_df.astype(str)
#                 #micro_df = micro_df.dtypes.astype(str)
                habit_df = habit_df.drop_duplicates()

    
                list_of_habit_name = [habitat_option]

                obtid, df_results = GetHabitObtid(jinja_env, 
                                                  es_url, 
                                                  index_es, 
                                                  list_of_habit_name)
                list_of_obtid = [i for i in obtid if i]
                st.write("Corresponding OBTID : ",list_of_obtid[0])

                micro_df = GetAllMicroWithObtidInTheirPath(jinja_env, 
                                                           es_url, 
                                                           index_es, 
                                                           client_es, 
                                                           list_of_obtid)

                micro_df = micro_df[COLSTOKEEP]
#                 micro_df = micro_df.astype(str)
                micro_df = micro_df.drop_duplicates()

#                 #s1 = pd.merge(df_micro, micro_df,  on=['document.form_microorganism', 'document.form_habitat'])
                microX_habitY = pd.merge(habit_df, micro_df,  
                                         how='inner', 
                                         left_on=['document.form_microorganism', 'document.form_habitat'], 
                                         right_on = ['document.form_microorganism', 'document.form_habitat'])

                microX_habitY = microX_habitY.drop_duplicates()
                st.dataframe(microX_habitY.head(10))
                Hbt = len(list(set(microX_habitY['document.form_habitat'].tolist())))
                TAXON = len(list(set(microX_habitY['document.form_microorganism'].tolist())))
                st.write("Unique HABITAT : ", Hbt)
                st.write("Unique TAXON : ", TAXON)
                
                st.write(repr(len(microX_habitY.drop_duplicates())) + '  documents dans le corpus')
                st.write('Size of query corpus : ', 
                         repr(round(sys.getsizeof(microX_habitY)/1000000,2))+ ' '+'Mb')

                st.write("Unique HABITAT - First@10", 
                         [i for i in list(set(microX_habitY['document.form_habitat'].tolist()))[:10]])
                st.write("Unique TAXON - First@10",
                         [i for i in list(set(microX_habitY['document.form_microorganism'].tolist()))[:10]])
#                 st.markdown(get_table_download_link_csv(microX_habitY), unsafe_allow_html=True)



            elif micro_option !='<select>' and habitat_option == '<select>' and all_micro and all_habit and Mi_DESC  and not (Hb_DESC):
                st.write('Looking for all  microorganism that live within the same HABITAT like : ', 
                         micro_option ,  '(descendant included)!')
                
                
                list_of_micro_name = [micro_option]
                list_of_micro_taxid, df_results = GetMicroNCBI(jinja_env, es_url, index_es, list_of_micro_name)
                st.write("Corresponding Taxid : ",list_of_micro_taxid[0])
                
                desclist, ascdlist, habit_df, list_of_micro_and_descd_habits = GetMicroDscHbt(jinja_env, 
                                                                                              es_url, 
                                                                                              index_es,
                                                                                              client_es,
                                                                                              list_of_micro_taxid)

                st.write("Nombre d'Habitats de ", micro_option, " et descendant : ", len(list_of_micro_and_descd_habits))
                st.write("Liste des Habitats - First@10", 
                         micro_option, " et descendant : " ,list_of_micro_and_descd_habits[:10])
#                 st.write( "list_of_micro_and_descd_habits :" , len(list_of_micro_and_descd_habits ))
                list_of_micro_and_descd_habits = [unidecode(i) for i in list_of_micro_and_descd_habits ]

                final_dfa = GetDfWithListOfHabitat(jinja_env, 
                                                  es_url, 
                                                  index_es, 
                                                  client_es,
                                                  list_of_micro_and_descd_habits[:1000])
                
                final_dfb = GetDfWithListOfHabitat(jinja_env, 
                                          es_url, 
                                          index_es, 
                                          client_es,
                                          list_of_micro_and_descd_habits[len(list_of_micro_and_descd_habits)-1000:])
            
                final_df = pd.concat([final_dfa, final_dfb], ignore_index=True)
        
                final_df = final_df[COLSTOKEEP]
#                 final_df = final_df.drop_duplicates()
                groupby_df = final_df.groupby(['document.form_microorganism'])['document.form_habitat'].nunique().reset_index()
                groupby_df = groupby_df.rename(columns={'document.form_habitat': 'document.form_habitat_count'})

                st.dataframe(final_df.head(10))
                st.write(repr(len(final_df)) + '  documents dans le corpus')
                st.write('Size of query corpus : ', repr(round(sys.getsizeof(final_df)/1000000,2))+ ' '+'Mb')

                st.dataframe(groupby_df.sort_values(by=['document.form_habitat_count'], ascending=False).head(30))
# #                 st.markdown(get_table_download_link_csv(final_df), unsafe_allow_html=True)



            elif habitat_option != '<select>' and micro_option !='<select>' and Mi_DESC and Hb_DESC and all_micro and not (all_habit):

                st.write('Looking for all microorganism that live in ', 
                         habitat_option, ' (descendant included) like ', 
                         micro_option, ' (descendant included)')

                list_of_micro_name = [micro_option]
                list_of_micro_taxid, df_results = GetMicroNCBI(jinja_env, es_url, index_es, list_of_micro_name)
                st.write("Corresponding Taxid : ",list_of_micro_taxid[0])
                
                
                desclist, ascdlist, habit_df, list_of_micro_and_descd_habits = GetMicroDscHbt(jinja_env, 
                                                                                              es_url, 
                                                                                              index_es,
                                                                                              client_es,
                                                                                              list_of_micro_taxid)  
#                 if len(habit_df)==0:
#                     st.write("No match for this query")
# #                     pass
#                 else:
                habit_df = habit_df[COLSTOKEEP]
                habit_df = habit_df.astype(str)
                #micro_df = micro_df.dtypes.astype(str)
                habit_df = habit_df.drop_duplicates()

                list_of_micro_and_descd_habits_ = [habitat_option]
                obtid, df_results = GetHabitObtid(jinja_env, 
                                                  es_url, 
                                                  index_es, 
                                                  list_of_micro_and_descd_habits_)
                list_of_obtid = [i for i in obtid if i]
                st.write("Corresponding OBTID : ",list_of_obtid[0])

                micro_df = GetAllMicroWithObtidInTheirPath(jinja_env, es_url, index_es, client_es, list_of_obtid)
#                 if micro_df is not None:
                micro_df = micro_df[COLSTOKEEP]
                micro_df = micro_df.astype(str)
                micro_df = micro_df.drop_duplicates()


                microX_habitY = pd.merge(habit_df, micro_df,  
                                         how='inner', 
                                         left_on=['document.form_microorganism','document.form_habitat'], 
                                         right_on = ['document.form_microorganism','document.form_habitat'])

                microX_habitY_df = microX_habitY.drop_duplicates()
                micro_and_desc_habit_in_Y = list(set(microX_habitY_df['document.form_habitat'].tolist()))

                micro_dfxy = GetDfWithListOfHabitat(jinja_env, 
                                              es_url, 
                                              index_es, 
                                              client_es,
                                              micro_and_desc_habit_in_Y)#[:1000])

                micro_dfxy = micro_dfxy[COLSTOKEEP]
                micro_dfxy = micro_dfxy.astype(str)
                micro_dfxy = micro_dfxy.drop_duplicates()
            

                st.write(len(micro_dfxy) , '  documents dans le corpus')
                st.write('Size of query corpus : ', repr(round(sys.getsizeof(micro_dfxy)/1000000,2))+ ' '+'Mb')
                st.write("Unique HABITAT - First@10",
                         [ i for i in list(set(micro_dfxy['document.form_habitat'].tolist()))[:10] if i])
#                 st.write(micro_and_desc_habit_in_Y)

                groupby_df_dsc = micro_dfxy['document.form_habitat'].groupby(
                                micro_dfxy['document.form_microorganism']).nunique().reset_index()

                groupby_df_dsc = groupby_df_dsc.rename(columns={'document.form_habitat': 'document.form_habitat_count'})               
                groupby_df_dsc_ = groupby_df_dsc[['document.form_microorganism','document.form_habitat_count']]

                st.dataframe(groupby_df_dsc.sort_values(by=['document.form_habitat_count'], ascending=False))

                st.markdown(get_table_download_link_csv(micro_dfxy), unsafe_allow_html=True)
                
if __name__ == '__main__':
    main()
