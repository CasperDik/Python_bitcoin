import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter

# import transaction data
transactions = pd.read_csv("transactions.csv")

# daily transactions
def daily_transactions(transactions):
    transactions = transactions.sort_values(by=["timestamp"])
    plt.plot(transactions["timestamp"], transactions["satoshis"])
    plt.show()

# count unique adresses
def unique_adresses(transactions):
    c0 = Counter(transactions["input_key"]).most_common(20)
    # c1 = Counter(transactions["output_key"]).most_common(20)

    x = []
    y = []
    for i in range(len(c0)):
        x.append(c0[i][0])
        y.append(c0[i][1])

    plt.bar(x, y)
    plt.title("20 adresses with most transactions")
    plt.xticks(x, y, rotation=90)
    plt.show()

    plt.bar(x[2:], y[2:])
    plt.title("Excluding 2 biggest")
    plt.xticks(x, y, rotation=90)
    plt.show()


def delete_exchange_transactions(transactions):
    with open('exchange_adresses.txt', "r") as f:
        lines = f.readlines()
    print(len(transactions))
    for exchange_adress in lines:
        transactions = transactions[transactions["output_key"] != exchange_adress]
        transactions = transactions[transactions["input_key"] != exchange_adress]
    print(len(transactions))
    return transactions
