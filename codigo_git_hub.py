from urllib.request import urlopen
from bs4 import BeautifulSoup
import ssl
import tabula
import urllib.request
import PyPDF2
import pandas as pd

# Controlar a data das atualizações do ficheiro

arq='data_atualização.txt'

def arquivoExiste(nome):
    try:
        a=open(nome, 'rt')
        a.close()
    except FileNotFoundError:
        return False
    else:
        return True

def criarArquivo(nome):
    try:
        a=open(nome,'wt+') # sinal + é o que cria o ficheiro se ele não existir
        a.close()
    except:
        print('Houve um erro na criação do arquivo!')
    else:
        print(f'Arquivo {nome} criado com sucesso!')

def Acrescenta(arq,file, data):
    try:
        a=open(arq,'at')
    except:
        print('Houve um erro na abertura do arquivo!')
    else:
        try:
            a.write(f'{file};{data}\n')
        except:
            print('Houve um erro ao escrever a data!')
        else:
            print(f'Novo registo adicionado.')
            a.close()

if not arquivoExiste(arq):
    criarArquivo(arq)

# Colocar sempre este código ao utilizar o package urllib

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# url dos devedores da AT

url = "https://static.portaldasfinancas.gov.pt/app/devedores_static/de-devedores.html"

print(f'retrieving:{format(url)}')
html = urlopen(url, context=ctx).read()
soup = BeautifulSoup(html, "html.parser")

#print(soup)
#print(soup.prettify()) #importante para ver o formato do html e para depois idenfificar o anchors

anchors = soup('iframe')
#print(anchors)
lista=list()
for tag in anchors:
    identif=tag.get('src')
    identif=identif.replace('.html','.pdf')
    #print(identif)
    url=('https://static.portaldasfinancas.gov.pt/app/devedores_static/listaF'+ identif)
    lista.append(url)
print(f'Número de ficheiros a extrair: {len(lista)}')

# A solução encontrada foi exportar os pdf's para depois os colocar num csv

print('***' * 30)
count=count_sing=count_colect=0
for i in lista:
    #print(i) #informação do URL
    filename=i[i.find('lista'):]
    print('Nome do ficheiro pdf:', filename) #nome do ficheiro -pdf
    urllib.request.urlretrieve(i, filename)

    file =i[i.find('lista'):]
    # Parte 1 - importar o pdf e transformar em um  dataframe

    ##coloca todas as páginas num único dataframe para poder trabalhar - table[0]

    try:
        table = tabula.read_pdf(file, java_options="-Dfile.encoding=UTF8", pages='all', multiple_tables=True,
                            encoding='utf-8', output_format="dataframe")
    except:
        print(f'\033[31mErro no ficheiro {file}.\033[m')
        print('***' * 30)
        continue
    #print(len(table))

    # df é um dataframe
    df = table[0]
    # print(type(df))
    # print(df)
    # testes ao dataframe
    # print(df.head(18))

    # Parte 2 - Ler o pdf para descobrir o montante e a data do ficheiro

    file_2 = open(file, "rb")
    reader = PyPDF2.PdfFileReader(file)

    if reader.isEncrypted:
        reader.decrypt('')

    page1 = reader.getPage(0)
    print("N.º de páginas do pdf:", reader.numPages)
    N_pages=reader.numPages
    pdfData = page1.extractText()
    # print(pdfData)
    montante = pdfData[pdfData.find('Devedores'):pdfData.find('•')]
    data = pdfData[pdfData.find('202'):pdfData.find('2021') + 10]
    Acrescenta(arq, filename, data)

    print('Montante:',montante)
    print('Data:', data)

    # Parte 3 - Trabalhar o dataframe e exportar o csv

    df.loc[:, 'Montante'] = montante
    df.loc[:, 'Data'] = data
    print(f'\33[34mExtração do ficheiro {filename} concluída com sucesso!\33[m')
    print('***' * 30)
    count=count+1

    if N_pages == 1:
        df_filtered = df
    else:
        df_filtered = df[df.iloc[:, 0].str.isnumeric()]  # retirar os cabeçalhos

    if filename[6:7]=='S':
        count_sing=count_sing+1
        if count_sing == 1:
            Contribuintes_singulares = pd.DataFrame(df_filtered)
        else:
            Contribuintes_singulares = pd.concat([Contribuintes_singulares,df_filtered])
    if filename[6:7]=='C':
        count_colect=count_colect+1
        if count_colect == 1:
            Contribuintes_colectivos = df_filtered
        else:
            Contribuintes_colectivos = pd.concat([Contribuintes_colectivos,df_filtered])

#df = pd.read_csv (r'Contribuintes_singulares.csv')
#df1 = pd.read_csv (r'Contribuintes_colectivos.csv')

#df = pd.read_csv (r'S:\IFM\Data\Controlo_qualidade\Daniel_Silva\Devedores_AT\Contribuintes_singulares.csv')
#df1 = pd.read_csv (r'S:\IFM\Data\Controlo_qualidade\Daniel_Silva\Devedores_AT\Contribuintes_colectivos.csv')

#Contribuintes_singulares = pd.concat([Contribuintes_singulares,df])
#Contribuintes_colectivos = pd.concat([Contribuintes_colectivos,df1])

#valores unicos

#Contribuintes_singulares = Contribuintes_singulares.drop_duplicates()
#Contribuintes_colectivos = Contribuintes_colectivos.drop_duplicates()

#Contribuintes_singulares.to_csv("S:\IFM\Data\Controlo_qualidade\Daniel_Silva\Devedores_AT\Contribuintes_singulares.csv", encoding='utf-8-sig', index=False)
#Contribuintes_colectivos.to_csv("S:\IFM\Data\Controlo_qualidade\Daniel_Silva\Devedores_AT\Contribuintes_colectivos.csv", encoding='utf-8-sig', index=False)

Contribuintes_singulares.to_csv("Contribuintes_singulares.csv", encoding='utf-8-sig', index=False)
Contribuintes_colectivos.to_csv("Contribuintes_colectivos.csv", encoding='utf-8-sig', index=False)



print(f'\033[32m{count} dos {len(lista)} ficheiros extraídos com sucesso!\033[m')
print(f'Ficheiro:Contribuintes_singulares.csv  - {len(Contribuintes_singulares)} registos adicionados.')
print(f'Ficheiro:Contribuintes_colectivos.csv  - {len(Contribuintes_colectivos)} registos adicionados.')
print('***' * 30)