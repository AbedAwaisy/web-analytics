from integrations.Helpper_Functions import *
import mysql.connector
import numpy as np
# websocket import, the file name is websocket
from integrations.WebSocket import *


class Insertion:
    def __init__(self, meta, yield_df=None, quality_df=None, cemical_df=None, exp=None):
        self.meta = meta
        self.y = yield_df
        self.q = quality_df
        self.c = cemical_df
        self.exp = exp

    async def insert(self):
        if len(self.meta) != 0:
            try:
                self.Insert_Meta()
                if self.exp:
                    if self.c is not None:
                        self.Insert_Cemical_With_Experiment()
                    if self.q is not None:
                        self.Insert_Quality_With_Experiment()
                    if self.y is not None:
                        self.Insert_Yield_With_Experiment()
                else:
                    if self.c is not None:
                        self.Insert_Cemical_Without_Experiment()
                    if self.q is not None:
                        self.Insert_Quality_Without_Experiment()
                    if self.y is not None:
                        self.Insert_Yield_Without_Experiment()

                await WebSocketHandler.send_message(f'File succesfully Integrated to the DBS. {len(self.meta)} rows inserted')

            except mysql.connector.Error as error:
                await WebSocketHandler.send_message('the data is already in the data base.')
        else:
            await WebSocketHandler.send_message('you gave the authirity to remove corrputed data which is all the file data.')

    async def Insert_Meta(self):
        # Establish MySQL connection (replace with your connection details)
        connection = connect()
        cursor = connection.cursor()
        # Define your table name
        table_name = 'ParcelMetaData'
        sql = f"INSERT INTO {table_name} (SampleID,ParcelID,SortingDate,HarvestDate,HerbName,SortingType,ParcelSize) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        # Insert data into MySQL table
        try:
            for index, row in self.meta.iterrows():
                cursor.execute(sql, (
                row['SampleID'], row['ParcelID'], row['SortingDate'], row['HarvestDate'], row['HerbName'],
                row['SortingType'], row['ParcelSize']))
            # Commit the transaction
            connection.commit()
            await WebSocketHandler.send_message(f"{len(self.meta)} rows inserted successfully into MySQL table: ", table_name)

        except mysql.connector.Error as error:
            # raise(error)
            await WebSocketHandler.send_message("Error inserting data:", error)

        finally:
            # Close the connection
            connection.close()

    async def Insert_Cemical_Without_Experiment(self):
        connection = connect()
        cursor = connection.cursor()
        # Define your table name
        table_name = 'Cemical_Without_Experiment'
        sql = f"INSERT INTO {table_name} (SampleID,TSS,TA,Glucose) VALUES (%s, %s, %s, %s)"
        # Insert data into MySQL table
        try:
            self.c.columns = ['SampleID', 'TSS', 'TA', 'Glucose']
            for index, row in self.c.iterrows():
                cursor.execute(sql, (row['SampleID'], row['TSS'], row['TA'], row['Glucose']))
            # Commit the transaction
            connection.commit()
            await WebSocketHandler.send_message(f"{len(self.c)} rows inserted successfully into MySQL table: ", table_name)

        except mysql.connector.Error as error:
            await WebSocketHandler.send_message("Error inserting data:", error)

        finally:
            # Close the connection
            connection.close()

    async def Insert_Cemical_With_Experiment(self):
        # Establish MySQL connection (replace with your connection details)
        connection = connect()
        cursor = connection.cursor()
        # Define your table name
        table_name = 'Cemical_With_Experiment'
        sql = f"INSERT INTO {table_name} (SampleID,ExperimentType,ExperimentParameter,TSS,TA,Glucose) VALUES (%s, %s, %s, %s, %s, %s)"
        # Insert data into MySQL table
        try:
            self.c.columns = ['SampleID', 'ExperimentType', 'ExperimentParameter', 'TSS', 'TA', 'Glucose']
            for index, row in self.c.iterrows():
                cursor.execute(sql, (
                row['SampleID'], row['ExperimentType'], row['ExperimentParameter'], row['TSS'], row['TA'],
                row['Glucose']))
            # Commit the transaction
            connection.commit()
            await WebSocketHandler.send_message(f"{len(self.c)} rows inserted successfully into MySQL table: ", table_name)

        except mysql.connector.Error as error:
            await WebSocketHandler.send_message("Error inserting data:", error)

        finally:
            # Close the connection
            connection.close()

    async def Insert_Quality_With_Experiment(self):
        # Establish MySQL connection (replace with your connection details)
        connection = connect()
        cursor = connection.cursor()
        # Define your table name
        table_name = 'Quality_With_Experiment'
        sql = f"INSERT INTO {table_name} (SampleID,ExperimentType,ExperimentParameter,Weight,FruietNumber,VineFreshness,Fallen,Cracked,Firm,Flexible,Soft,Rotten,ColorDefect,Missing,VineRot,Shade,GeneralAppearance,ColorVirus,ScratchesVirus) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)"
        # Insert data into MySQL table
        try:
            self.q.columns = ['SampleID', 'ExperimentType', 'ExperimentParameter', 'Weight', 'FruietNumber',
                              'VineFreshness', 'Fallen', 'Cracked', 'Firm', 'Flexible', 'Soft', 'Rotten', 'ColorDefect',
                              'Missing', 'VineRot', 'Shade', 'GeneralAppearance', 'ColorVirus', 'ScratchesVirus']
            for index, r in self.q.iterrows():
                cursor.execute(sql, (
                r['SampleID'], r['ExperimentType'], r['ExperimentParameter'], r['Weight'], r['FruietNumber'],
                r['VineFreshness'], r['Fallen'], r['Cracked'], r['Firm'], r['Flexible'], r['Soft'], r['Rotten'],
                r['ColorDefect'], r['Missing'], r['VineRot'], r['Shade'], r['GeneralAppearance'], r['ColorVirus'],
                r['ScratchesVirus']))
            # Commit the transaction
            connection.commit()
            await WebSocketHandler.send_message(f"{len(self.q)} rows inserted successfully into MySQL table: ", table_name)

        except mysql.connector.Error as error:
            await WebSocketHandler.send_message("Error inserting data:", error)

        finally:
            # Close the connection
            connection.close()

    async def Insert_Quality_Without_Experiment(self):
        connection = connect()
        cursor = connection.cursor()
        # Define your table name
        table_name = 'Quality_Without_Experiment'
        sql = f"INSERT INTO {table_name} (SampleID,Weight,FruietNumber,VineFreshness,Fallen,Cracked,Firm,Flexible,Soft,Rotten,ColorDefect,Missing,VineRot,Shade,GeneralAppearance,ColorVirus,ScratchesVirus) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)"
        # Insert data into MySQL table
        try:
            self.q.columns = ['SampleID', 'Weight', 'FruietNumber', 'VineFreshness', 'Fallen', 'Cracked', 'Firm',
                              'Flexible', 'Soft', 'Rotten', 'ColorDefect', 'Missing', 'VineRot', 'Shade',
                              'GeneralAppearance', 'ColorVirus', 'ScratchesVirus']
            for index, r in self.q.iterrows():
                cursor.execute(sql, (
                r['SampleID'], r['Weight'], r['FruietNumber'], r['VineFreshness'], r['Fallen'], r['Cracked'], r['Firm'],
                r['Flexible'], r['Soft'], r['Rotten'], r['ColorDefect'], r['Missing'], r['VineRot'], r['Shade'],
                r['GeneralAppearance'], r['ColorVirus'], r['ScratchesVirus']))
            # Commit the transaction
            connection.commit()
            await WebSocketHandler.send_message(f"{len(self.q)} rows inserted successfully into MySQL table: ", table_name)

        except mysql.connector.Error as error:
            await WebSocketHandler.send_message("Error inserting data:", error)

        finally:
            # Close the connection
            connection.close()

    async def Insert_Yield_With_Experiment(self):
        # Establish MySQL connection (replace with your connection details)
        connection = connect()
        cursor = connection.cursor()
        # Define your table name
        table_name = 'Yield_With_Experiment'
        sql = f"INSERT INTO {table_name} (SampleID,ExperimentType,ExperimentParameter,ClusterHarvesd,SingleHarvesd,SingleHarvesdNumber,GreenFruits,CrackedFruits,BlackPitDefect,BlackPitDefectNumber,Others,PlantProtection,Virus) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s)"
        # Insert data into MySQL table
        try:
            self.y.replace({np.nan: None}, inplace=True)
            self.y.columns = ['SampleID', 'ExperimentType', 'ExperimentParameter', 'ClusterHarvesd', 'SingleHarvesd',
                              'SingleHarvesdNumber', 'GreenFruits', 'CrackedFruits', 'BlackPitDefect',
                              'BlackPitDefectNumber', 'Others', 'PlantProtection', 'Virus']
            for index, row in self.y.iterrows():
                cursor.execute(sql, (
                row['SampleID'], row['ExperimentType'], row['ExperimentParameter'], row['ClusterHarvesd'],
                row['SingleHarvesd'], row['SingleHarvesdNumber'], row['GreenFruits'], row['CrackedFruits'],
                row['BlackPitDefect'], row['BlackPitDefectNumber'], row['Others'], row['PlantProtection'],
                row['Virus']))
            # Commit the transaction
            connection.commit()
            await WebSocketHandler.send_message(f"{len(self.y)} rows inserted successfully into MySQL table: ", table_name)

        except mysql.connector.Error as error:
            await WebSocketHandler.send_message("Error inserting data:", error)

        finally:
            # Close the connection
            connection.close()

    async def Insert_Yield_Without_Experiment(self):
        # Establish MySQL connection (replace with your connection details)
        connection = connect()
        cursor = connection.cursor()
        # Define your table name
        table_name = 'Yield_Without_Experiment'
        sql = f"INSERT INTO {table_name} (SampleID,ClusterHarvesd,SingleHarvesd,SingleHarvesdNumber,GreenFruits,CrackedFruits,BlackPitDefect,BlackPitDefectNumber,Others,PlantProtection,Virus) VALUES (%s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s)"
        # Insert data into MySQL table
        try:
            self.y.replace({np.nan: None}, inplace=True)
            self.y.columns = ['SampleID', 'ClusterHarvesd', 'SingleHarvesd', 'SingleHarvesdNumber', 'GreenFruits',
                              'CrackedFruits', 'BlackPitDefect', 'BlackPitDefectNumber', 'Others', 'PlantProtection',
                              'Virus']
            for index, row in self.y.iterrows():
                cursor.execute(sql, (
                row['SampleID'], row['ClusterHarvesd'], row['SingleHarvesd'], row['SingleHarvesdNumber'],
                row['GreenFruits'], row['CrackedFruits'], row['BlackPitDefect'], row['BlackPitDefectNumber'],
                row['Others'], row['PlantProtection'], row['Virus']))
            # Commit the transaction
            connection.commit()
            await WebSocketHandler.send_message(f"{len(self.y)} rows inserted successfully into MySQL table: ", table_name)

        except mysql.connector.Error as error:
            await WebSocketHandler.send_message("Error inserting data:", error)

        finally:
            # Close the connection
            connection.close()