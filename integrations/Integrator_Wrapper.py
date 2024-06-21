from integrations.Validation_Meta import *
from integrations.Validation_Quality import *
from integrations.Meta_Extraction import *
from integrations.Yield_Extraction import *
from integrations.Quality_Extraction import *
from integrations.Chemical_Extraction import *
from integrations.Insertion import *
from integrations.Validation_Yield import Validation_Yield
from integrations.Helpper_Functions import *

# I used the function names: send_message to send and, receive_message to receive


class Integrator:
    def __init__(self, websocket: WebSocket, file_path: str):
        self.websocket = websocket
        self.file_path = file_path
        self.df = pd.read_csv(file_path)
        self.exp = None

    async def send_message(self, message: str):
        await self.websocket.send_text(message)

    async def receive_message(self) -> str:
        return await self.websocket.receive_text()

    async def validate_file(self):
        columns = columns_mapper(self.df)
        await self.send_message("Does this file have an Experiment column? (Yes/No)")
        has_experiment = await self.receive_message()
        experiment_column_name = None
        if has_experiment.lower() == "yes":
            await self.send_message("Please enter the Experiment column name:")
            experiment_column_name = await self.receive_message()
            self.exp = experiment_column_name

            columns_count = len(self.df.columns)
            rows_count = len(self.df)
            unique_values_sort = self.df[columns["סוג מיון"]].unique().tolist()

            await self.send_message(
                f"Processing file '{self.file_path}' with Experiment column '{experiment_column_name}'."
            )

            await self.send_message(
                f"File '{self.file_path}' has {columns_count} columns and {rows_count} rows and sort types {unique_values_sort}. Proceed with integration? (Yes/No)"
            )
            proceed = await self.receive_message()

            if proceed.lower() == "yes":
                await self.integrate_file()
            else:
                await self.send_message("Integration cancelled by user.")
        else:
            await self.send_message("No Experiment column. Processing skipped.")
            columns_count = len(self.df.columns)
            rows_count = len(self.df)
            unique_values_sort = self.df[columns["סוג מיון"]].unique().tolist()

            await self.send_message(
                f"Processing file '{self.file_path}' with Experiment column '{experiment_column_name}'."
            )

            await self.send_message(
                f"File '{self.file_path}' has {columns_count} columns and {rows_count} rows and sort types {unique_values_sort}. Proceed with integration? (Yes/No)"
            )
            proceed = await self.receive_message()

            if proceed.lower() == "yes":
                await self.integrate_file()
            else:
                await self.send_message("Integration cancelled by user.")

    async def integrate_file(self):
        try:
            # Here, instantiate and run the necessary classes and methods to integrate the file.
            # For example, let's assume we have a `run_integration` method:
            await self.run_integration()
            await self.send_message("File inserted successfully.")
        except Exception as e:
            await self.send_message(f"An error occurred during integration: {e}")

    async def run_integration(self):
        # Implement the integration logic here
        data = self.df.copy()
        exp = ' '.join(self.exp.split())
        columns = columns_mapper(data)
        data[columns['סוג מיון']] = data[columns['סוג מיון']].apply(lambda x: x.replace(" ", "").lower())

        validator_meta = Validation_Meta(data)
        await validator_meta.validate_sampleID()
        await validator_meta.validate_harvest_date()
        await validator_meta.validate_sorting_date()
        # validator_meta.validate_date_gap()
        await validator_meta.validate_date_order()
        await validator_meta.validate_parcel_size()
        await validator_meta.validate_exists_herb_name_info()
        validator_meta.validate_herb_name_info_with_dbms()

        Meta_Extractor = Meta_Extraction(validator_meta.df)
        meta = Meta_Extractor.extract_Meta()

        c, q, y = None, None, None
        sorting_types = list(validator_meta.df[columns['סוג מיון']].unique())
        sorting_types = [s.replace(" ", "").lower() for s in sorting_types]
        for s in sorting_types:
            if s in ['cemical', 'כימי', 'cemicalכימי', 'כימיcemical']:
                c_df = validator_meta.df[validator_meta.df[columns['סוג מיון']] == s]
                Cemical_Extractor = Cemical_Extraction(c_df, exp)
                c = Cemical_Extractor.extract_Cemical()

            elif s in ['quality', 'איכות', 'qualityאיכות', 'איכותquality']:
                q_df = validator_meta.df[validator_meta.df[columns['סוג מיון']] == s]
                Validateor_Quality = Validation_Quality(q_df)
                await Validateor_Quality.validate_weight()
                await Validateor_Quality.validate_vine_root()
                await Validateor_Quality.validate_vine_freshness()
                await Validateor_Quality.validate_shade()
                await Validateor_Quality.validate_scratches_virus()
                await Validateor_Quality.validate_general_appereance()
                await Validateor_Quality.validate_color_virus()
                Quality_Extractor = Quality_Extraction(Validateor_Quality.df, exp)
                q = Quality_Extractor.extract_Quality()

            elif s in ['yield', 'רגיל', 'yieldרגיל', 'רגילyield']:
                y_df = validator_meta.df[validator_meta.df[columns['סוג מיון']] == s]
                Validateor_Yield = Validation_Yield(y_df)
                await Validateor_Yield.validate_cluster_harvest()
                await Validateor_Yield.validate_single_harvest()
                await Validateor_Yield.validate_single_harvest_number()
                Yield_Extractor = Yield_Extraction(Validateor_Yield.df, exp)
                y = Yield_Extractor.extract_Yield()

            else:
                await self.send_message(f'error sorting type {s} not integrated with the sysytem')

        meta_data = Meta_to_keep(c, q, y, meta)
        Inserter = Insertion(meta_data, yield_df=y, quality_df=q, cemical_df=c, exp=exp)
        await Inserter.insert()
