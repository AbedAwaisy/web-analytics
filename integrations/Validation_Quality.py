class Validation_Quality:
    def __init__(self, df):
        self.df = df
        self.columns = columns_mapper(self.df)
        self.n = len(self.df)

    def validate_wieght(self, thresh=3.2):
        col = self.columns['משקל weight']
        invalid_rows_df = self.df[self.df[col].apply(lambda x: math.log10(x)) > thresh]
        if not invalid_rows_df.empty:
            authority = input(
                f"{len(invalid_rows_df)} out of {self.n} Invalid weights value(s) founded in the weight column ({col}). the log 10 of the weight value must be less than {thresh}, give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[col].notna()]
                self.n = len(self.df)
            else:
                indices = [i + 2 for i in list(invalid_rows_df.index)]
                print(
                    f"\n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices}")
                raise ValueError(f"Invalid weight value(s) founded in the weight column ({col}).")

    def validate_vine_freshness(self, low=0, up=5):
        col = self.columns['רעננות שדרהvine freshness']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]

        if not invalid_rows_df.empty:
            authority = input(
                f"{len(invalid_rows_df)} out of {self.n} Invalid vine freshness value(s) founded in the vine freshness column ({col}). the values must be in range({low}, {up}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[col].notna()]
                self.n = len(self.df)
            else:
                indices = [i + 2 for i in list(invalid_rows_df.index)]
                print(
                    f"\n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices}")
                raise ValueError(f"Invalid vine freshness value(s) founded in the vine freshness column ({col}).")

    def validate_vine_root(self, low=0, up=5):
        col = self.columns['רקבון שזרהvine rot']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]
        if not invalid_rows_df.empty:
            authority = input(
                f"{len(invalid_rows_df)} out of {self.n} Invalid vine root value(s) founded in the vine root column ({col}). the values must be in range({low}, {up}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[col].notna()]
                self.n = len(self.df)
            else:
                indices = [i + 2 for i in list(invalid_rows_df.index)]
                print(
                    f"\n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices}")
                raise ValueError(f"Invalid vine root value(s) founded in the vine root column ({col}).")

    def validate_shade(self, low=0, up=5):
        col = self.columns['גווןshade']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]
        if not invalid_rows_df.empty:
            authority = input(
                f"{len(invalid_rows_df)} out of {self.n} Invalid shade value(s) founded in the shade column ({col}). the values must be in range({low}, {up}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[col].notna()]
                self.n = len(self.df)
            else:
                indices = [i + 2 for i in list(invalid_rows_df.index)]
                print(
                    f"\n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices}")
                raise ValueError(f"Invalid shade value(s) founded in the shade column ({col}).")

    def validate_general_appereance(self, low=0, up=5):
        col = self.columns['מראה כלליgeneral appearance']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]
        if not invalid_rows_df.empty:
            authority = input(
                f"{len(invalid_rows_df)} out of {self.n} Invalid general appereance value(s) founded in the general appereance column ({col}). the values must be in range({low}, {up}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[col].notna()]
                self.n = len(self.df)
            else:
                indices = [i + 2 for i in list(invalid_rows_df.index)]
                print(
                    f"\n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices}")
                raise ValueError(
                    f"Invalid general appereance value(s) founded in the general appereance column ({col}).")

    def validate_color_virus(self, low=0, up=5):
        col = self.columns['וירוס צבעcolor virus']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]
        if not invalid_rows_df.empty:
            authority = input(
                f"{len(invalid_rows_df)} out of {self.n} color virus value(s) founded in the color virus column ({col}). the values must be in range({low}, {up}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[col].notna()]
                self.n = len(self.df)
            else:
                indices = [i + 2 for i in list(invalid_rows_df.index)]
                print(
                    f"\n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices}")
                raise ValueError(f"Invalid color virus value(s) founded in the color virus column ({col}).")

    def validate_scratches_virus(self, low=0, up=5):
        col = self.columns['וירוס שריטותscratches virus']
        invalid_rows_df = self.df[(low > self.df[col]) | (self.df[col] > up)]
        if not invalid_rows_df.empty:
            authority = input(
                f"{len(invalid_rows_df)} out of {self.n} scratches virus value(s) founded in the scratches virus column ({col}). the values must be in range({low}, {up}), give me authority to drop those rows or fix the file and try again: ")
            if authority == 'yes':
                self.df = self.df[self.df[col].notna()]
                self.n = len(self.df)
            else:
                indices = [i + 2 for i in list(invalid_rows_df.index)]
                print(
                    f"\n the invalid samples indices are (the indices as it appears in the Excel file.): \n {indices}")
                raise ValueError(f"Invalid scratches virus value(s) founded in the scratches virus column ({col}).")
