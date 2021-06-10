"""
DF names outlined in 'columns' field as well as with 'id_' with an _ delimiter
""";

def make_dfs():
    #df = input(prompt = 'Enter variable name for master sql df: ')
    try:
        df_strings = [i for i in list(master.columns) if 'columns_' in i]
        df_fields = []
        for i in range(0,len(df_strings)):
            temp_clean = list(master[df_strings[i]].unique())
            for n in temp_clean:
                if n != None:
                    df_fields.append(n.split(','))
                else:
                    pass
        df_names = [i[1] for i in [i.split('columns_') for i in df_strings]]
    except:
        print('Variable name not specified. Please try again.')
        return
    df_names=sorted(df_names)
    df_fields=sorted(df_fields)
    for i in range(0,len(df_names)):
        globals()[str(df_names[i])] = master[df_fields[i]]
