import pandas
import json
import numpy
from pandas import ExcelWriter
import os
import traceback

path = os.path.abspath(os.getcwd())

def clean_column_names(cols):
    cols = cols.str.replace(r'[^\x00-\x7F]+', " ", regex=True)\
                  .str.replace(" +", " ", regex=True)\
                  .str.strip()\
                  .str.replace(" ", "_")\
                  .str.upper()
    return cols

def set_types_fillna(df1, cols):
#     can handle str, float, int, bool, datetime
    for col, spec in cols.items():
        data_type = spec.get("data_type")
        default = spec.get("default")
        if data_type == "datetime":
            df1[col] = pandas.to_datetime(df1[col], errors='coerce')
        if data_type in ['int', 'float']:
            df1[col] = pandas.to_numeric(df1[col], errors='coerce')
            df1[col] = df1[col].fillna(default).astype(data_type)
        if data_type == "str":
            df1[col] = df1[col].fillna(default).astype(data_type)
    return df1

def save_xls(dict_df, path):
    """
    Save a dictionary of dataframes to an excel file, with each dataframe as a seperate page
    """
    writer = ExcelWriter(path)
    for key in dict_df:
        dict_df[key].to_excel(writer, key, index=False)

    writer.save()

def generate_diff(df1, df2):
    df_all = pandas.concat([df1, df2], axis='columns', keys=['Input', 'Output'])
    df_final = df_all.swaplevel(axis='columns')[df1.columns[:]]

    def highlight_diff(data, color='yellow'):
        attr = 'background-color: {}'.format(color)
        other = data.xs('First', axis='columns', level=-1)
        return pandas.DataFrame(numpy.where(data.ne(other, level=0), attr, ''),
                            index=data.index, columns=data.columns)

    df_final = df_final.style.apply(highlight_diff, axis=None)
    return df_final


def pipeline(filename, sheetname, xls, cols_conf):
    columns = list(cols_conf.get("columns").keys())
    
    df0  = pandas.read_excel(xls, sheet_name=sheet_name)
    df1 = df0
    df1.columns = clean_column_names(df1.columns)
    df1 = df1[[i for i in columns]]
    df1 = set_types_fillna(df1, cols_conf.get("columns"))
    df_diff = generate_diff(df0, df1)
    return df1, df_diff

if __name__ == "__main__":

    with open('config.json') as f:
        config_data = json.load(f)

    status = True
    for conf in config_data:
        try:
            filename = conf.get("filename")
            sheets = conf.get("sheet_names")
            xls = pandas.ExcelFile(filename)
            df_dict = {}
            df_diff_dic = {}
            for sheet_name, conf_cols in sheets.items():
                df_out, df_diff = pipeline(filename, sheet_name, xls, conf_cols)
                df_dict[sheet_name] = df_out
            save_xls(df_dict, os.path.join(path, filename.split('.')[0] + "_output.xlsx"))
        
        except Exception as e:
            status = False
            traceback.print_exc()
            print(e)
    
    if status:
        print("pipeline run successfully")

    #     some issue with writing the styler object, skipping diff output for now
    #         df_diff_dic[sheet] = df_diff
    #     save_xls(df_diff_dic, os.path.join(path, filename.split('.')[0] + "_diff.xlsx"))
