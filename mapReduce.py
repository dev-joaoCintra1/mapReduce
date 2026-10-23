# from math import sqrt

# ARQUIVO = r"C:\Users\User\Downloads\forestfireinput.csv"
# N_BLOCOS = 4

# COL_TEMP = 9                      # coluna 9 (1 = primeira)
# # colunas numericas; mes (3) e dia (4) sao texto e ficam de fora
# COLUNAS = {1: "X", 2: "Y", 5: "FFMC", 6: "DMC", 7: "DC", 8: "ISI",
#            10: "RH", 11: "vento", 12: "chuva", 13: "area"}


# # ---------- 1) LER E DIVIDIR (o "HDFS" quebrando o arquivo em blocos) ----------
# def dividir_em_blocos(linhas, n):
#     return [linhas[i::n] for i in range(n)]


# # ---------- 2) MAP: roda em cada bloco, linha por linha ----------
# def mapper(linha):
#     """Recebe uma linha e devolve uma lista de pares (chave, valor).
#     chave = nome da coluna | valor = (1, x, y, x*x, y*y, x*y)
#     Devolve [] se a linha estiver incompleta ou invalida."""
#     campos = linha.strip().split(",")
#     if len(campos) != 13:                       # linha incompleta -> descarta
#         return []
#     try:
#         x = float(campos[COL_TEMP - 1])         # temperatura
#         pares = []
#         for col, nome in COLUNAS.items():
#             y = float(campos[col - 1])
#             pares.append((nome, (1, x, y, x * x, y * y, x * y)))
#         return pares
#     except ValueError:                          # algum campo nao e numero -> descarta a linha toda
#         return []


# # ---------- 3) SHUFFLE: agrupa os valores pela mesma chave ----------
# def shuffle(todos_os_pares):
#     grupos = {}
#     for chave, valor in todos_os_pares:
#         grupos.setdefault(chave, []).append(valor)
#     return grupos                               # {"RH": [v1, v2, ...], "DC": [...]}


# # ---------- 4) REDUCE: soma tudo de uma chave e calcula Pearson ----------
# def reducer(chave, valores):
#     n   = sum(v[0] for v in valores)
#     sx  = sum(v[1] for v in valores)
#     sy  = sum(v[2] for v in valores)
#     sxx = sum(v[3] for v in valores)
#     syy = sum(v[4] for v in valores)
#     sxy = sum(v[5] for v in valores)

#     numerador   = n * sxy - sx * sy
#     denominador = sqrt((n * sxx - sx ** 2) * (n * syy - sy ** 2))
#     r = numerador / denominador if denominador != 0 else float("nan")
#     return chave, r, n


# # ---------------------------- PROGRAMA PRINCIPAL ----------------------------
# if __name__ == "__main__":
#     with open(ARQUIVO, encoding="utf-8") as f:
#         linhas = [l for l in f.read().splitlines() if l.strip()]

#     blocos = dividir_em_blocos(linhas, N_BLOCOS)

#     todos_os_pares = []
#     for i, bloco in enumerate(blocos, start=1):
#         pares_do_bloco = []
#         for linha in bloco:
#             pares_do_bloco += mapper(linha)         # MAP em cada linha do bloco
#         print(f"Bloco {i}: {len(bloco)} linhas -> {len(pares_do_bloco)} pares")
#         todos_os_pares += pares_do_bloco

#     grupos = shuffle(todos_os_pares)                # SHUFFLE

#     print("\nCorrelacao de Pearson com a temperatura (coluna 9):")
#     for chave, valores in grupos.items():
#         nome, r, n = reducer(chave, valores)        # REDUCE
#         print(f"  temp x {nome:6s} r = {r:+.4f}   (n={n})")




# ======================================================================
# CORRELACAO DA TEMPERATURA (COLUNA 9) COM AS OUTRAS COLUNAS - MAPREDUCE
#
# ORDEM DO PROGRAMA:
#   1) LER      - le o arquivo CSV inteiro
#   2) DIVIDIR  - quebra o arquivo em blocos (um bloco por "slave")
#   3) MAP      - cada slave le suas linhas e gera numeros para somar
#   4) SHUFFLE  - junta os numeros de cada coluna
#   5) REDUCE   - soma tudo e calcula a correlacao
# ======================================================================
from math import sqrt

ARQUIVO = r"C:\Users\User\Downloads\forestfireinput.csv"
NUM_SLAVES = 4

# Colunas que serao comparadas com a temperatura.
# As colunas 3 e 4 (mes e dia) sao texto, entao nao entram.
COLUNAS = [1, 2, 5, 6, 7, 8, 10, 11, 12, 13]


# ----------------------------------------------------------------------
# PASSO 1 - LER
# Abre o arquivo e coloca cada linha numa lista.
# ----------------------------------------------------------------------
def ler_arquivo():
    arquivo = open(ARQUIVO)
    linhas = arquivo.read().splitlines()   # uma linha do CSV = um item da lista
    arquivo.close()
    return linhas


# ----------------------------------------------------------------------
# PASSO 2 - DIVIDIR
# Reparte as linhas entre os slaves, como o HDFS divide o arquivo em blocos.
# Linha 0 -> slave 0, linha 1 -> slave 1, ..., linha 4 -> slave 0, etc.
# ----------------------------------------------------------------------
def dividir(linhas, quantidade):
    blocos = []
    for i in range(quantidade):
        blocos.append([])                  # cria um bloco vazio para cada slave
    for posicao in range(len(linhas)):
        slave = posicao % quantidade       # resto da divisao: 0,1,2,3,0,1,2,3...
        blocos[slave].append(linhas[posicao])
    return blocos


# ----------------------------------------------------------------------
# PASSO 3 - MAP
# Recebe UMA linha e devolve uma lista de pares (coluna, numeros).
# Os "numeros" sao [1, x, y, x*x, y*y, x*y], onde:
#     x = temperatura   y = valor da outra coluna
# Esses numeros sao as parcelas que o REDUCE vai somar depois.
# ----------------------------------------------------------------------
def mapa(linha):
    campos = linha.split(",")              # separa a linha pelas virgulas

    # Garantia de integridade 1: a linha precisa ter as 13 colunas
    if len(campos) != 13:
        return []                          # linha incompleta: ignora

    pares = []
    try:
        x = float(campos[8])               # coluna 9 = posicao 8 (comeca em 0)
        for c in COLUNAS:
            y = float(campos[c - 1])
            pares.append((c, [1, x, y, x * x, y * y, x * y]))
    except ValueError:
        # Garantia de integridade 2: algum campo nao e numero.
        # Ignora a linha INTEIRA, para x e y nunca ficarem desencontrados.
        return []
    return pares


# Roda o MAP em todas as linhas de UM slave.
def executar_slave(numero, bloco):
    pares_do_slave = []
    for linha in bloco:
        pares_do_slave = pares_do_slave + mapa(linha)
    print("  Slave", numero, "-> recebeu", len(bloco), "linhas e gerou",
          len(pares_do_slave), "pares")
    return pares_do_slave


# ----------------------------------------------------------------------
# PASSO 4 - SHUFFLE
# Junta, numa lista so, os numeros que pertencem a mesma coluna.
# Antes: misturado.  Depois: (coluna 5: [...], coluna 6: [...], ...)
# ----------------------------------------------------------------------
def shuffle(todos_os_pares, coluna):
    numeros_da_coluna = []
    for c, numeros in todos_os_pares:
        if c == coluna:
            numeros_da_coluna.append(numeros)
    return numeros_da_coluna


# ----------------------------------------------------------------------
# PASSO 5 - REDUCE
# Soma todas as parcelas de UMA coluna e aplica a formula da correlacao:
#   r = (n*Sxy - Sx*Sy) / raiz( (n*Sxx - Sx*Sx) * (n*Syy - Sy*Sy) )
# onde S significa "soma de todos".
# ----------------------------------------------------------------------
def reduz(lista):
    n = 0      # quantidade de linhas
    sx = 0     # soma de x
    sy = 0     # soma de y
    sxx = 0    # soma de x*x
    syy = 0    # soma de y*y
    sxy = 0    # soma de x*y
    for v in lista:
        n = n + v[0]
        sx = sx + v[1]
        sy = sy + v[2]
        sxx = sxx + v[3]
        syy = syy + v[4]
        sxy = sxy + v[5]

    cima = n * sxy - sx * sy
    baixo = sqrt((n * sxx - sx * sx) * (n * syy - sy * sy))
    return cima / baixo


# ----------------------------------------------------------------------
# MAIN - executa os passos na ordem
# ----------------------------------------------------------------------
def main():
    print("PASSO 1 - LER o arquivo")
    linhas = ler_arquivo()
    print("  total de linhas lidas:", len(linhas))

    print("\nPASSO 2 - DIVIDIR em", NUM_SLAVES, "blocos")
    blocos = dividir(linhas, NUM_SLAVES)
    for i in range(NUM_SLAVES):
        print("  Bloco do slave", i, ":", len(blocos[i]), "linhas")

    print("\nPASSO 3 - MAP (cada slave processa o seu bloco)")
    todos_os_pares = []
    for i in range(NUM_SLAVES):
        todos_os_pares = todos_os_pares + executar_slave(i, blocos[i])

    print("\nPASSO 4 e 5 - SHUFFLE e REDUCE (uma coluna por vez)")
    print("\nRESULTADO - Correlacao da Coluna 9 (temperatura) com:")
    for c in COLUNAS:
        numeros = shuffle(todos_os_pares, c)     # passo 4
        r = reduz(numeros)                       # passo 5
        print("  Coluna", c, "=", round(r, 4))


main()
