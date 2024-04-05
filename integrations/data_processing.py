from sqlite3 import Error

from database import *
import pandas as pd
from app.dependencies import get_db_connection

def columns_mapper(df):
    columns = df.columns.to_list()
    clean_columns = {}
    for c in columns:
        if 'Unnamed:' not in c:
            # c_hat = ' '.join(c.split())
            c_hat = c.strip()
            clean_columns[c_hat] = c

    return clean_columns


def extract_meta(df):
    columns = columns_mapper(df)
    df_HerbName = HerbName_Integrator(df)
    df_ParcelSize = ParcelSize_Integrator(df_HerbName)

    SampleID = df_ParcelSize[columns['מזהה דגימה']].astype(int)
    ParcelID = df_ParcelSize[columns['חלקה']].astype(int)
    SortingDate = pd.to_datetime(df_ParcelSize[columns['תאריך מיון']], errors='coerce')
    HarvestDate = pd.to_datetime(df_ParcelSize[columns['תאריך קטיף']], errors='coerce').dt.date
    HerbName = df_ParcelSize[['HerbName']].astype(str)
    ParcelSize = df_ParcelSize[columns['גודל חלקה במר']].astype(float)

    meta = pd.DataFrame({'SampleID': SampleID, 'ParcelID': ParcelID, 'SortingDate': SortingDate, 'HarvestDate': HarvestDate,'HerbName': HerbName, 'ParcelSize': ParcelSize})

    return meta, df_ParcelSize


def extract_yield(df, experiment):
    columns = columns_mapper(df)
    removed_indecis = []
    result_columns = ['יצוא אשכולות', 'יצוא בודדים', 'ירוקים', 'סדוקים', 'שחור פיטם', 'שחור פיטם מספר', 'אחרים',
                      'הגנת הצומח', 'וירוס']
    df, temp_removed_indecis = FruietNumber_Integrator(df, columns['יצוא בודדים מספר'])
    removed_indecis += temp_removed_indecis

    for c in result_columns:
        df, temp_removed_indecis = remove_outliers_iqr(df, columns[c])
        removed_indecis += temp_removed_indecis

    Yield_df = pd.DataFrame({'SampleID': df[columns['מזהה דגימה']].astype(int),
                             'ClusterHarvesd': df[columns['משקל weight']].astype(float),
                             'SingleHarvesd': df[columns['מספר פרות']].astype(float),
                             'SingleHarvesdNumber': df[columns['רעננות שדרהvine freshness']].astype(int),
                             'GreenFruits': df[columns['נפליםfallen']].astype(float),
                             'CrackedFruits': df[columns['מפוצציםcracked']].astype(float),
                             'BlackPitDefect': df[columns['מוצקיםfirm']].astype(float),
                             'BlackPitDefectNumber': df[columns['גמישיםflexible']].astype(int),
                             'Others': df[columns['רכיםsoft']].astype(float),
                             'PlantProtection': df[columns['רקוביםrotten']].astype(float),
                             'Virus': df[columns['חריגי צבעcolor defect']].astype(float)})
    if experiment:
        Yield_df['ExperimentType'] = experiment.strip()
        if Yield_df[columns[experiment.strip()]].isna().sum() > 0:
            Yield_df[columns[experiment.strip()]].fillna('Control')

        Yield_df['ExperimentParameter'] = Yield_df[columns[experiment.strip()]]
        experiment_cols = {'ExperimentType': Yield_df['ExperimentType'].astype(str),
                           'ExperimentParameter': Yield_df['ExperimentParameter'].astype(str)}
        first_column_index = Yield_df.columns.get_loc(Yield_df.columns[0])
        # Add the new columns after the first column
        for col_name, col_data in experiment_cols.items():
            Yield_df.insert(first_column_index + 1, col_name, col_data)

        Insert_Quality_With_Experiment(Yield_df)

    else:
        Insert_Quality_Without_Experiment(Yield_df)

    return removed_indecis


def ParcelSize_Integrator(df):
    columns = columns_mapper(df)
    column_name = columns['גודל חלקה במר']
    none_count = df[column_name].isna().sum()

    if is_empty_dbs():
        # df.dropna(subset=[column_name], inplace=True)
        if (df[column_name] == 0).all() or none_count == len(df):
            raise ValueError(
                'The values of column גודל חלקה במר is not valid, all Parcels size must not be 0 or empty. please correct the values and try again.')
        elif (df[column_name] == 0).any() or none_count > 0:
            authority = input('The values of column גודל חלקה במר is not valid, some of the Parcels with a size of 0 or empty. please correct the values or give me authority to delete those rows: ')
            if authority == 'y':
                df = df[df[columns['גודל חלקה במר']] != 0]
                df = df[df[columns['גודל חלקה במר']].notnull()]
            else:
                raise ValueError('please correct the values and try again.')

    else:
        column_names = [columns['חלקה'], columns['גודל חלקה במר']]
        values_to_check = df[column_names].values.tolist()
        connection = connect()
        cursor = connection.cursor()
        query = f"SELECT {', '.join(column_names)} ParceMetaData"
        cursor.execute(query)
        database_values = cursor.fetchall()
        cursor.close()
        database_values = [tuple(row) for row in database_values]
        database_praces = [row[0] for row in database_values]

        # insure that the prace size in the dbs is the same with the new file based on the prace id.
        for prace, prace_size in database_values:
            df.loc[df[columns['חלקה']] == prace, columns['גודל חלקה במר']] = prace_size

        values_not_in_database = [value for value in values_to_check if value[0] not in database_praces]
        not_valid = count_not_valid(values_not_in_database)

        if not_valid == len(df) or none_count == len(df):
            raise ValueError(
                'The values of column גודל חלקה במר is not valid, all Parcels with a size of 0 or empty cell. please correct the values and try again.')
        elif not_valid > 0 or none_count > 0:
            authority = input(
                'The values of column גודל חלקה במר is not valid, some of the Parcels with a size of 0 or empty cell. please correct the values or give me authority to delete those rows: ')
            if authority == 'y':
                df = df[df[columns['גודל חלקה במר']] != 0]
                df = df[df[columns['גודל חלקה במר']].notnull()]
            else:
                raise ValueError('please correct the values and try again.')

    return df


def extract_quality(df, experiment):
    columns = columns_mapper(df)
    removed_indecis = []
    result_columns = ['משקל weight', 'רעננות שדרהvine freshness', 'נפליםfallen', 'מפוצציםcracked', 'מוצקיםfirm',
                      'גמישיםflexible', 'רכיםsoft', 'רקוביםrotten', 'חריגי צבעcolor defect', 'רקבון שזרהvine rot',
                      'חסריםmissing', 'גווןshade', 'מראה כלליgeneral appearance', 'וירוס צבעcolor virus',
                      'וירוס שריטותscratches virus']
    for c in result_columns:
        df, temp_removed_indecis = remove_outliers_iqr(df, columns[c])
        removed_indecis += temp_removed_indecis

    df[columns['מספר פרות']] = df.apply(update_column, axis=1)
    Quality_df = pd.DataFrame({'SampleID': df[columns['מזהה דגימה']].astype(int),
                               'Weight': df[columns['משקל weight']].astype(float),
                               'FruietNumber': df[columns['מספר פרות']].astype(int),
                               'VineFreshness': df[columns['רעננות שדרהvine freshness']].astype(int),
                               'Fallen': df[columns['נפליםfallen']].astype(int),
                               'Cracked': df[columns['מפוצציםcracked']].astype(int),
                               'Frim': df[columns['מוצקיםfirm']].astype(int),
                               'Flexible': df[columns['גמישיםflexible']].astype(int),
                               'Soft': df[columns['רכיםsoft']].astype(int),
                               'Rotten': df[columns['רקוביםrotten']].astype(int),
                               'ColorDefect': df[columns['חריגי צבעcolor defect']].astype(int),
                               'Missing': df[columns['חסריםmissing']].astype(int),
                               'VineRot': df[columns['רקבון שזרהvine rot']].astype(float),
                               'Shade': df[columns['גווןshade']].astype(float),
                               'GeneralAppearance': df[columns['מראה כלליgeneral appearance']].astype(float),
                               'ColorVirus': df[columns['וירוס צבעcolor virus']].astype(float),
                               'ScratchesVirus': df[columns['וירוס שריטותscratches virus']].astype(float)})
    if experiment:
        Quality_df['ExperimentType'] = experiment.strip()
        if Quality_df[columns[experiment.strip()]].isna().sum() > 0:
            Quality_df[columns[experiment.strip()]].fillna('Control')

        Quality_df['ExperimentParameter'] = Quality_df[columns[experiment.strip()]]
        experiment_cols = {'ExperimentType': Quality_df['ExperimentType'].astype(str),
                           'ExperimentParameter': Quality_df['ExperimentParameter'].astype(str)}
        first_column_index = Quality_df.columns.get_loc(Quality_df.columns[0])
        # Add the new columns after the first column
        for col_name, col_data in experiment_cols.items():
            Quality_df.insert(first_column_index + 1, col_name, col_data)

        Insert_Quality_With_Experiment(Quality_df)

    else:
        Insert_Quality_Without_Experiment(Quality_df)

    return removed_indecis


def HerbName_Extractor(df):
    columns = columns_mapper(df)
    knh = [c for c in columns if ('כ' in c) and ('נ' in c) and ('ה' in c)]
    rokev = [c for c in columns if ('ר' in c) and ('כ' in c) and ('ב' in c)]

    if ('זן' in columns.keys()) and df[columns['זן']].isnull().sum() != len(df):
        df[columns['זן']] = df[columns['זן']].astype(str)
        distribution_dict_zan = df[columns['זן']].value_counts().to_dict()
        herbs_df = list(distribution_dict_zan.keys())
        if (len(knh) == 0) and (len(rokev) == 0):
            df['HerbName'] = df.apply(lambda row: row[columns['זן']].strip(), axis=1)
        elif len(knh) != 0 and len(rokev) == 0:
            df['HerbName'] = df.apply(lambda row: row[columns['זן']].strip() + ' ' + row[columns[knh[0]]].strip(),
                                      axis=1)
        elif len(rokev) != 0 and len(knh) == 0:
            distribution_dict_rokev = df[columns[rokev[0]]].value_counts().to_dict()
            if len(distribution_dict_rokev) == 2:
                df['HerbName'] = df.apply(
                    lambda row: 'לא מורכב' + ' ' + row[columns['זן']].strip() if ('ל' in row[columns[rokev[0]]]) and (
                                'ל' in row[columns[rokev[0]]]) and ('א' in row[columns[rokev[0]]]) else 'מורכב' + ' ' +
                                                                                                        row[columns[
                                                                                                            'זן']].strip(),
                    axis=1)
            else:  # row[columns['זן']].strip() if row[columns[rokev[0]]].strip() == 'לא מורכב' else
                df['HerbName'] = df.apply(lambda row: row[columns[rokev[0]]].strip() + ' ' + row[columns['זן']].strip(),
                                          axis=1)

        none_count = df['HerbName'].isna().sum()
        if len(herbs_df) == 1 and none_count / len(df) <= 0.3:
            df['HerbName'].fillna(df['HerbName'].unique()[0], inplace=True)
        else:
            authority = input('There is values of column זן that is missing, please correct the values or give me authority to delete those rows: ')
            if authority == 'y':
                df = df[df['HerbName'].notnull()]
            else:
                raise ValueError('please correct the values and try again.')

    elif ('שם צמח' in columns.keys()) and df[columns['שם צמח']].isnull().sum() != len(df):
        df[columns['שם צמח']] = df[columns['שם צמח']].astype(str)
        distribution_dict_zan = df[columns['שם צמח']].value_counts().to_dict()
        herbs_df = list(distribution_dict_zan.keys())
        if (len(knh) == 0) and (len(rokev) == 0):
            df['HerbName'] = df.apply(lambda row: row[columns['שם צמח']].strip(), axis=1)
        elif len(knh) != 0 and len(rokev) == 0:
            df['HerbName'] = df.apply(lambda row: row[columns['שם צמח']].strip() + ' ' + row[columns[knh[0]]].strip(),
                                      axis=1)
        elif len(rokev) != 0 and len(knh) == 0:
            distribution_dict_rokev = df[columns[rokev[0]]].value_counts().to_dict()
            if len(distribution_dict_rokev) == 2:
                df['HerbName'] = df.apply(lambda row: 'לא מורכב' + ' ' + row[columns['שם צמח']].strip() if ('ל' in row[
                    columns[rokev[0]]]) and ('ל' in row[columns[rokev[0]]]) and ('א' in row[
                    columns[rokev[0]]]) else 'מורכב' + ' ' + row[columns['שם צמח']].strip(), axis=1)
            else:  # row[columns['שם צמח']].strip() if row[columns[rokev[0]]].strip() == 'לא מורכב' else
                df['HerbName'] = df.apply(
                    lambda row: row[columns[rokev[0]]].strip() + ' ' + row[columns['שם צמח']].strip(), axis=1)

        none_count = df['HerbName'].isna().sum()
        if len(herbs_df) == 1 and none_count / len(df) <= 0.3:
            df['HerbName'].fillna(df['HerbName'].unique()[0], inplace=True)
        else:
            authority = input(
                'There is values of column שם צמח that is missing, please correct the values or give me authority to delete those rows: ')
            if authority == 'y':
                df = df[df['HerbName'].notnull()]
            else:
                raise ValueError('please correct the values and try again.')

    elif len(rokev) != 0 and len(
            knh) != 0:  # row[columns[knh[0]]].strip() if row[columns[rokev[0]]].strip() == 'לא מורכב' else
        df['HerbName'] = df.apply(lambda row: row[columns[rokev[0]]].strip() + ' ' + row[columns[knh[0]]].strip(),
                                  axis=1)


    else:
        raise ValueError('missing herb name information, please add the missing information to the file and try again.')

    return df


def count_not_valid(lst):
    c = 0
    for item in lst:
        if item[1] == 0:
            c += 1
    return c

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
    cleaned_df = df[(df[column_name] >= lower_bound) & (df[column_name] <= upper_bound)]

    # Get the indices of the removed rows
    removed_indices = df.index.difference(cleaned_df.index)

    return cleaned_df, removed_indices


def Integrate(file_path):
    experiment = input("Please enter Experiment column name if there is an Experiment in the file: ")
    df = pd.read_csv(file_path)
    columns = columns_mapper(df)
    # meta
    meta, df = extract_meta(df)
    sorting_types = df[columns['סוג מיון']].unique().tolist()
    for s in sorting_types:
        if s == 'איכותquality':
            indices_to_remove = extract_quality(df, experiment)
            meta.drop(indices_to_remove, inplace=True)
        elif s == 'כימי':
            indices_to_remove = extract_cemical(df, experiment)
            meta.drop(indices_to_remove, inplace=True)
        else:
            indices_to_remove = extract_yield(df, experiment)
            meta.drop(indices_to_remove, inplace=True)

    Insert_Meta(meta)
    print('File succesfully Integrated to the DBS.')
    return


from difflib import SequenceMatcher


def string_similarity(s1, s2):
    # Create a SequenceMatcher object
    if s1.isdigit():
        return 0

    seq_matcher = SequenceMatcher(None, s1, s2)

    # Get the similarity ratio
    similarity_ratio = seq_matcher.ratio()

    return similarity_ratio


def HerbName_Integrator(df, column_name='HerbName', table_name='ParceMetaData'):
    columns = columns_mapper(df)
    my_df = HerbName_Extractor(df)
    if is_empty_dbs():
        return my_df
    else:
        herbs_dbs = get_herbs_dbs()
        herbs_df = list(my_df['HerbName'].unique())
        for h1 in herbs_df:
            score = []
            for h2 in herbs_dbs:
                s = string_similarity(h1, h2)
                score.append(s)
            most_similar = max(score)
            if most_similar >= 0.8:
                herb = herbs_dbs[score.index(most_similar)]
                my_df.loc[my_df['HerbName'] == h1, 'HerbName'] = herb

        return my_df


def extract_cemical(df, experiment):
    columns = columns_mapper(df)
    removed_indecis = []
    TA_df, TA_removed_indecis = remove_outliers_iqr(df, columns['TA'])
    TSS_df, TSS_removed_indecis = remove_outliers_iqr(TA_df, columns['TSS'])
    G_df, G_removed_indecis = remove_outliers_iqr(TSS_df, columns['גלוקוז'])
    removed_indecis = TA_removed_indecis + TSS_removed_indecis + G_removed_indecis

    if experiment:
        G_df['ExperimentType'] = experiment.strip()
        if G_df[columns[experiment.strip()]].isna().sum() > 0:
            G_df[columns[experiment.strip()]].fillna('Control')
        G_df['ExperimentParameter'] = G_df[columns[experiment.strip()]]
        cemical_df = pd.DataFrame(
            {'SampleID': G_df[columns['מזהה דגימה']].astype(int), 'ExperimentType': G_df['ExperimentType'].astype(str),
             'ExperimentParameter': G_df['ExperimentParameter'].astype(str), 'TSS': G_df[columns['TSS']].astype(float),
             'TA': G_df[columns['TA']].astype(float), 'Glucose': G_df[columns['גלוקוז']].astype(float)})
        Insert_Cemical_With_Experiment(cemical_df)
    else:
        cemical_df = pd.DataFrame(
            {'SampleID': G_df[columns['מזהה דגימה']].astype(int), 'TSS': G_df[columns['TSS']].astype(float),
             'TA': G_df[columns['TA']].astype(float), 'Glucose': G_df[columns['גלוקוז']].astype(float)})
        Insert_Cemical_Without_Experiment(cemical_df)

    return removed_indecis


def FruietNumber_Integrator(df, c):
    threshold = len(df) * 0.1  # Define a threshold

    # Count the number of values that are not 0 or 20
    count_different_values = df[df[c].apply(lambda x: x not in [0, 20])].shape[0]

    if count_different_values <= threshold:
        authority = input(
            "All or almost all values in the FruietNumber column are either 0 or 20 which is ambiguis. give me the authority to make the values None or fix the column values and try again: ")
        if authority == 'y':
            df[c] = None
            return df, []
        raise ValueError('All or almost all values in the FruietNumber column are either 0 or 20 which is ambiguis.')
    else:
        df1, removed_indecis = remove_outliers_iqr(df, c)
        return df1, removed_indecis
#########################################
def is_empty_dbs(table_name = 'ParceMetaData'):
    connection = connect()
    cursor = connection.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    row_count = cursor.fetchone()["COUNT(*)"]
    cursor.close()
    if row_count == 0:
        print(f"The table '{table_name}' is empty.")
        return  True

    return False


def Insert_Meta(meta):
  # Establish MySQL connection (replace with your connection details)
  connection = connect()
  cursor = connection.cursor()
  # Define your table name
  table_name = 'ParceMetaData'
  sql = f"INSERT INTO {table_name} (SampleID,ParcelID,SortingDate,HarvestDate,HerbName,SortingType,ParcelSize) VALUES (%s, %s, %s, %s, %s, %s, %s)"
  # Insert data into MySQL table
  try:
      for index, row in meta.iterrows():
          cursor.execute(sql, (row['SampleID'], row['ParcelID'], row['SortingDate'], row['HarvestDate'], row['HerbName'], row['SortingType'], row['ParcelSize']))
      # Commit the transaction
      connection.commit()
      print("Data inserted successfully into MySQL table:", table_name)

      print("Data inserted successfully!")

  except Error as error:
      print("Error inserting data:", error)

  finally:
      # Close the connection
      connection.close()


def Insert_Cemical_Without_Experiment(cemical1):
  connection = connect()
  cursor = connection.cursor()
  # Define your table name
  table_name = 'Cemical_Without_Experiment'
  sql = f"INSERT INTO {table_name} (SampleID,TSS,TA,Glucose) VALUES (%s, %s, %s, %s)"
  # Insert data into MySQL table
  try:
      cemical1.columns = ['SampleID','TSS','TA','Glucose']
      for index, row in cemical1.iterrows():
          cursor.execute(sql, (row['SampleID'], row['TSS'], row['TA'], row['Glucose']))
      # Commit the transaction
      connection.commit()
      print("Data inserted successfully into MySQL table:", table_name)

      print("Data inserted successfully!")

  except Error as e:
      print("Error inserting data:", e)

  finally:
      # Close the connection
      connection.close()

def Insert_Cemical_With_Experiment(cemical2):
  # Establish MySQL connection (replace with your connection details)
  connection = connect()
  cursor = connection.cursor()
  # Define your table name
  table_name = 'Cemical_With_Experiment'
  sql = f"INSERT INTO {table_name} (SampleID,ExperimentType,ExperimentParameter,TSS,TA,Glucose) VALUES (%s, %s, %s, %s, %s, %s)"
  # Insert data into MySQL table
  try:
      cemical2.columns = ['SampleID','ExperimentType','ExperimentParameter','TSS','TA','Glucose']
      for index, row in cemical2.iterrows():
          cursor.execute(sql, (row['SampleID'], row['ExperimentType'], row['ExperimentParameter'], row['TSS'], row['TA'], row['Glucose']))
      # Commit the transaction
      connection.commit()
      print("Data inserted successfully into MySQL table:", table_name)

      print("Data inserted successfully!")

  except Error as error:
      print("Error inserting data:", error)

  finally:
      # Close the connection
      connection.close()

def Insert_Quality_With_Experiment(Quality_df):
  # Establish MySQL connection (replace with your connection details)
  connection = connect()
  cursor = connection.cursor()
  # Define your table name
  table_name = 'Quality_With_Experiment'

  sql = f"INSERT INTO {table_name} (SampleID,ExperimentType,ExperimentParameter,Weight,FruietNumber,VineFreshness,Fallen,Cracked,Firm,Flexible,Soft,Rotten,ColorDefect,Missing,VineRot,Shade,GeneralAppearance,ColorVirus,ScratchesVirus) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)"
  # Insert data into MySQL table
  try:
      Quality_df.columns = ['SampleID','ExperimentType','ExperimentParameter','Weight','FruietNumber','VineFreshness','Fallen','Cracked','Firm','Flexible','Soft','Rotten','ColorDefect','Missing','VineRot','Shade','GeneralAppearance','ColorVirus','ScratchesVirus']
      for index, r in Quality_df.iterrows():
          cursor.execute(sql, (r['SampleID'], r['ExperimentType'], r['ExperimentParameter'], r['Weight'],r['FruietNumber'],r['VineFreshness'],r['Fallen'],r['Cracked'],r['Firm'],r['Flexible'],r['Soft'],r['Rotten'],r['ColorDefect'],r['Missing'],r['VineRot'],r['Shade'],r['GeneralAppearance'],r['ColorVirus'],r['ScratchesVirus']))
      # Commit the transaction
      connection.commit()
      print("Data inserted successfully into MySQL table:", table_name)

      print("Data inserted successfully!")

  except Error as error:
      print("Error inserting data:", error)

  finally:
      # Close the connection
      connection.close()
def Insert_Quality_Without_Experiment(Quality_df):
  connection = connect()
  cursor = connection.cursor()
  # Define your table name
  table_name = 'Quality_Without_Experiment'
  sql = f"INSERT INTO {table_name} (SampleID,Weight,FruietNumber,VineFreshness,Fallen,Cracked,Firm,Flexible,Soft,Rotten,ColorDefect,Missing,VineRot,Shade,GeneralAppearance,ColorVirus,ScratchesVirus) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)"
  # Insert data into MySQL table
  try:
      Quality_df.columns = ['SampleID','Weight','FruietNumber','VineFreshness','Fallen','Cracked','Firm','Flexible','Soft','Rotten','ColorDefect','Missing','VineRot','Shade','GeneralAppearance','ColorVirus','ScratchesVirus']
      for index, r in Quality_df.iterrows():
          cursor.execute(sql, (r['SampleID'],r['Weight'],r['FruietNumber'],r['VineFreshness'],r['Fallen'],r['Cracked'],r['Firm'],r['Flexible'],r['Soft'],r['Rotten'],r['ColorDefect'],r['Missing'],r['VineRot'],r['Shade'],r['GeneralAppearance'],r['ColorVirus'],r['ScratchesVirus']))
      # Commit the transaction
      connection.commit()
      print("Data inserted successfully into MySQL table:", table_name)

      print("Data inserted successfully!")

  except Error as error:
      print("Error inserting data:", error)

  finally:
      # Close the connection
      connection.close()


def get_herbs_dbs(table_name = 'ParceMetaData', column_name = 'HerbName'):
    connection = connect()
    cursor = connection.cursor()
    cursor.execute(f"SELECT DISTINCT {column_name} FROM {table_name}")
    unique_values = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return unique_values


def connect():
    return get_db_connection()


#Integrate("../Experiment209Results.csv")


