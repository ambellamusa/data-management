class Configurations:
    folder_path = r"D:" #Insert file path wheref files are located to be parsed
    high_column = "High"
    low_column = "Low"


class Data:
    def __init__(self):
        self.TableName = "AL_Expectations"
        self.ClientID = ""
        self.FileName = ""
        self.UploadDate = ""
        self.Columns = ""
        self.Min = 0
        self.Max = 0

    def display_values(self):
        print("---------------------------------------------------")
        print(self.TableName)
        print(self.ClientID, self.FileName, self.UploadDate)
        print(self.Min, self.Max)
        print(self.Columns)

