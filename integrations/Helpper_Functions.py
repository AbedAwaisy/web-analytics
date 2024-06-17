import mysql.connector
from difflib import SequenceMatcher
import numpy as np
import Levenshtein


# helpper function [no need to do any thing]

def connect():
    #Create a connection to the MySQL server
    conn = mysql.connector.connect(user='root', password='Q4P.M+0t>u$>As+0', host='34.165.192.24', database="Example")
    return conn



def is_empty_dbs(table_name = 'ParcelMetaData'):
    connection = connect()
    cursor = connection.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cursor.fetchone()[0]
    cursor.close()
    connection.close()
    if row_count == 0:
        return  True

    return False


def get_herbs_dbs(table_name = 'ParcelMetaData', column_name = 'HerbName'):
    connection = connect()
    cursor = connection.cursor()
    cursor.execute(f"SELECT DISTINCT {column_name} FROM {table_name}")
    unique_values = [row[0] for row in cursor.fetchall()]
    cursor.close()
    connection.close()
    return unique_values

def get_id_range(table_name = 'ParcelMetaData', column_name = 'SampleID'):
    connection = connect()
    cursor = connection.cursor()
    cursor.execute(f"SELECT MIN({column_name}) as min_value, MAX({column_name}) as max_value FROM {table_name}")
    unique_values = [row for row in cursor.fetchall()]
    cursor.close()
    connection.close()
    return unique_values

def get_existing_ids(table_name = 'ParcelMetaData', id_column = 'SampleID'):
    connection = connect()
    cursor = connection.cursor()
    cursor.execute(f"SELECT {id_column} FROM {table_name}")
    existing_ids = [row[0] for row in cursor.fetchall()]
    cursor.close()
    connection.close()
    return existing_ids

def columns_mapper(df):
  columns = df.columns.to_list()
  clean_columns = {}
  for c in columns:
    if 'Unnamed:' not in c:
      #c_hat = ' '.join(c.split())
      c_hat = c.strip()
      clean_columns[c_hat] = c

  return clean_columns

def count_not_valid(lst):
  c = 0
  for item in lst:
    if item[1] == 0:
      c += 1
  return c

def string_similarity(s1, s2):
    # Create a SequenceMatcher object
    if s1.isdigit():
      return 0

    seq_matcher = SequenceMatcher(None, s1, s2)

    # Get the similarity ratio
    similarity_ratio = seq_matcher.ratio()

    return similarity_ratio

def remove_outliers_iqr(df, column_name):
    # Calculate the first quartile (Q1) and third quartile (Q3)
    Q1 = df[column_name].quantile(0.25)
    Q3 = df[column_name].quantile(0.75)

    # Calculate the interquartile range (IQR)
    IQR = Q3 - Q1

    # Define the lower and upper bounds to detect outliers
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    # Filter the DataFrame to remove outliers and get the cleaned DataFrame
    cleaned_df = df.loc[(df[column_name] >= lower_bound) & (df[column_name] <= upper_bound)]

    return cleaned_df

def remove_outliers_zscore(df, column_name, threshold=3):
    # Calculate the Z-score for each data point
    z_scores = np.abs((df[column_name] - df[column_name].mean()) / df[column_name].std())
    
    # Remove the outliers
    df_filtered = df[z_scores <= threshold]

    # Return the filtered DataFrame and the indices of the removed outliers
    return df_filtered

def cap_values(df, columns_to_check, reference_column):
    for col in columns_to_check:
        df[col] = df.apply(lambda row: min(row[col], row[reference_column]), axis=1)
    return df

def generate_levenshtein_distance_1(word):
    letters = '-אבגדהוזחטיכלמוןצעפתקרשת'
    splits = [(word[:i], word[i:]) for i in range(len(word) + 1)]

    deletes = [L + R[1:] for L, R in splits if R]
    inserts = [L + c + R for L, R in splits for c in letters]
    substitutes = [L + c + R[1:] for L, R in splits if R for c in letters]

    return set(deletes + inserts + substitutes)

def generate_levenshtein_distance_1_2(word):
    distance_1_words = generate_levenshtein_distance_1(word).union(word)
    distance_2_words = set(word)

    for w in distance_1_words:
        distance_2_words.update(generate_levenshtein_distance_1(w))

    # Remove the original word and any words that are not exactly distance 2
    distance_2_words = {w for w in distance_2_words if Levenshtein.distance(word, w) == 2}

    return distance_1_words.union(distance_2_words)

def candidate_column(column_name, columns):
    Levenshtein_candidates = generate_levenshtein_distance_1_2(column_name)
    candidate = None
    for cand in Levenshtein_candidates:
        temp_cand = 'זן' + ' ' + cand
        if cand in columns.keys():
            candidate = cand
        elif temp_cand in columns:
            candidate = temp_cand
        if candidate is not None:
            break
        
        
    return candidate
            
def FruietNumber_Integrator(df, c):
  threshold = len(df) * 0.1  # Define a threshold
  data = df.copy()
  # Count the number of values that are not 0 or 20
  count_different_values = df[c].nunique()
  if count_different_values <= threshold:
      
    data[c] = -1
    return data
      
  else:
     df1 = remove_outliers_iqr(df, c)
     return df1


def Meta_to_keep(df1, df2, df3, df4):
    # Union of df1, df2, and df3 based on the 'id' column
    union_ids = set()
    
    if df1 is not None:
        union_ids.update(df1['SampleID'].unique())
    
    if df2 is not None:
        union_ids.update(df2['SampleID'].unique())
    
    if df3 is not None:
        union_ids.update(df3['SampleID'].unique())
    
    # Keep only rows in df4 where the 'id' is in the union of df1, df2, and df3
    df4_filtered = df4[df4['SampleID'].isin(union_ids)]
    return df4_filtered

            
def check_valid_single_harvest(row, col1, col2):
    if row[col1] == 0 and row[col2] != 0:
        row[col2] = 0
    elif row[col1] != 0 and row[col2] == 0:
        row[col2] = -1
    return row

def update_single_harvest_number(df, col1, col2):
    return df.apply(check_valid_single_harvest, axis=1, col1=col1, col2=col2)
    

