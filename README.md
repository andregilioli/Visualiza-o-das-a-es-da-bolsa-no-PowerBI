# Visualizacoes_das_acoes_da_bolsa_no_PowerBI
#Projeto de visualização das ações da bovespa no power BI com ETL em Python.

# Inicialmente será demonstrado o processo de ETL dos dados:

# 1) Baixar dados dos anos desejados no site <https://www.b3.com.br/pt_br/market-data-e-indices/servicos-de-dados/market-data/historico/mercado-a-vista/series-historicas/> os dados estão em formato JSON.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

# 2) Um dos processos de ETL e conversão realizada para ".csv" foi feita através do seguinte codigo em Pyhton (no Jupyter Notebook):

#iniciando um processo de ETL com pyhton

#importando pandas:

    import pandas as pd

Criando a função para criar o data frame:

    def read_files (path, name_files, year_date, type_file ):

      _file = f'{path}{name_file}{year_date}.{type_file}'
    
      colspecs =[(2,10),
                 (10,12),
                 (12,24),
                 (27,39),
                 (56,69),
                 (69,82),
                 (108,121),
                 (152,170),
                 (170,188)
           
    ]

    names = ['data_pregao','codbdi','sigla_acao','nome_acao','preco_abertura','preco_maximo','preco_minimo','preco_fechamento','qtd_negocios','volume_negocios']

    df = pd.read_fwf(_file, colspecs = colspecs, names = names, skiprows =1)

    return df
    

#filtra a coluna codbdi, seleciona somente as linhas que possuem valor igual a 2
#exclui a coluna selecionada, no caso "codbdi", o valor "1" é usado quando se deseja remover coluna

      def filter_stocks(df):
   
        df = df [df['codbdi']== 2]  
        df = df.drop (['codbdi'], 1) 
    
      return df


#realizando ajuste na unidade de medida da data

    def parse_date (df):

      df['data_pregao']= pd.to_datetime(df['data_pregao'],format='%Y%m%d') #Essa linha transforma a data no modelo AAAA-MM-DD

      return df


#realizando ajuste nas unidade de medidas os campos numéricos. A casa dos centavos foram desconsideradas no relatorio bruto


     def parse_values (df):

        df['preco_abertura'] = (df['preco_abertura']/100).astype(float).round(2)
        df['preco_maximo'] = (df['preco_maximo']/100).astype(float).round(2)
        df['preco_minimo'] = (df['preco_minimo']/100).astype(float).round(2)
        df['preco_fechamento'] = (df['preco_fechamento']/100).astype(float).round(2)
      
    
     return df
    
#Unindo os arquivos

      def concat_files (path, name_file, year_date, type_file, final_file):
    
      for i, y in enumerate (year_date):
        
        df = read_files (path, name_file, y, type_file)
        df = filter_stocks (df)
        df = parse_date (df)
        df = parse_values (df)
        
        if i == 0:
            df_final = df
        else:
            df_final = pd.concat([df_final,df])
            
        
    df_final.to_csv(f'{path}//{final_file}', index = False)
    
    
#Executando programa para realizar a ETL

    year_date = ['2018','2019','2020','2021']

    path = f'C://Users//andre//'

    name_file = 'COTAHIST_A'

    type_file = 'txt'

    final_file = 'all_bovespa.csv'

    concat_files (path, name_file, year_date, type_file, final_file)
    
#O arquivo .csv foi gerado unificando os 4 arquivos json referente as ações historicas de 2018/2019/2020/2021.    

-------------------------------------------------------------------------------------------------------------------------------------------------------------------------


