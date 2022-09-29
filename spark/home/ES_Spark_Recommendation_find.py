from elasticsearch import Elasticsearch

# test your ES instance is running
es = Elasticsearch('http://172.28.0.3:9200')
es.info(pretty=True)


# In[2]:


import pandas as pd

def vector_query(query_vec, category,vector_field, cosine=False):
    """
    Construct an Elasticsearch script score query using `dense_vector` fields
    
    The script score query takes as parameters the query vector (as a Python list)
    
    Parameters
    ----------
    query_vec : list
        The query vector
    vector_field : str
        The field name in the document against which to score `query_vec`
    q : str, optional
        Query string for the search query (default: '*' to search across all documents)
    cosine : bool, optional
        Whether to compute cosine similarity. If `False` then the dot product is computed (default: False)
     
    Note: Elasticsearch cannot rank negative scores. Therefore, in the case of the dot product, a sigmoid transform
    is applied. In the case of cosine similarity, 1.0 is added to the score. In both cases, documents with no 
    factor vectors are ignored by applying a 0.0 score.
    
    The query vector passed in will be the user factor vector (if generating recommended items for a user)
    or product factor vector (if generating similar items for a given item)
    """
    
    if cosine:
        score_fn = "doc['{v}'].size() == 0 ? 0 : cosineSimilarity(params.vector, '{v}') + 1.0"
    else:
        score_fn = "doc['{v}'].size() == 0 ? 0 : sigmoid(1, Math.E, -dotProduct(params.vector, '{v}'))"
       
    score_fn = score_fn.format(v=vector_field, fn=score_fn)
    
    return {
    "script_score": {
        "query" : { 
            "bool" : {
                  "filter" : {
                        "term" : {
                          "main_category" : category
                        }
                    }
            }
        },
        "script": {
            "source": score_fn,
            "params": {
                "vector": query_vec
            }
        }
    }
}


def get_similar(the_id, num=10, index="products", vector_field='model_factor'):
    """
    Given a item id, execute the recommendation script score query to find similar items,
    ranked by cosine similarity. We return the `num` most similar, excluding the item itself.
    """
    response = es.get(index=index, id=the_id)
    src = response['_source']
    if vector_field in src:
        query_vec = src[vector_field]
        category = src['main_category']
        q = vector_query(query_vec, category,vector_field, cosine=True)
#         print(q)
        results = es.search(index=index, query=q)
        hits = results['hits']['hits']
        return src,hits[1:num+1]

def display_similar(the_id, num=10, es_index="products"):
    """
    Display query product, together with similar product and similarity scores, in a table
    """
    product, recs = get_similar(the_id, num, es_index)
       
    display(HTML("<h2>Get similar products for:</h2>"))
    display(HTML("<h4>%s (ASIN - %s)</h4>" % (product['title'], product['asin'])))
    display(HTML("<br>"))
    display(HTML("<h2>People who liked this product also liked these:</h2>"))
    sim_html = "<table border=0>"
    i = 0
    pd_data = []
    for rec in recs:
        r_score = rec['_score']
        r_title = rec['_source']['title']
        r = {}
        r['asin'] = rec['_source']['asin']
        r['title'] = r_title
        r['score'] = r_score
        pd_data.append(r)
        #r_im_url = next(iter(rec['_source']['image']), '')
        sim_html += "<tr><td><h5>%s</h5></td><td><h5>%2.3f</h5></td></tr>" % (r_title, r_score)
        i += 1
    sim_html += "</table>"
    pd.set_option('display.max_colwidth', None) 
    pd_df = pd.DataFrame (pd_data)
    #display(HTML(pd_df.to_html()))
    display(HTML(sim_html))


# In[3]:


from IPython.display import Image, HTML, display
data = display_similar('B001A5HT94', num=5)

