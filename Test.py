import pandas as pd


start = time.clock()
print('Test started...')
dataframe = pd.read_csv('randomized-transactions-202009.psv',chunksize = 17000000,sep='|')

for i in range(10):
    df = next(dataframe)
    if i == 0:
        res = df.groupby(["code_magasin"]).sum('prix')
    else:
        res=res.append(df.groupby(["code_magasin"]).sum('prix'))
    del(df)

true_top_50 =list(res.groupby("code_magasin").sum('prix').sort_values(by=['prix'],ascending=False)[:50].index.values)

df_top_50_calculated = list(pd.read_csv('top-50-stores.csv')['code_magasin'])

if(true_top_50 ==df_top_50_calculated):
    print('Test validated')
else:
    print('Test rejected')

end = time.clock()
print('The process took: ' + str(end - start)+' seconds')
