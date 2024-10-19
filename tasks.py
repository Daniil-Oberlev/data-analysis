from matplotlib import pyplot as plt
import pandas as pd
from time import time
from dataset_utils import ensure_dataset_loaded


# Задание 1: Первые 10 строк и количество строк/столбцов
@ensure_dataset_loaded()
def task_1(df):
    print("task_1")
    print("\n")

    print("Первые 10 строк датасета:")
    print(df.head(10))
    print(f"\nВсего строк: {df.shape[0]}, Всего столбцов: {df.shape[1]}")

    print("\n\n\n")


# Задание 2: Уникальные города
@ensure_dataset_loaded(required_columns=["City"])
def task_2(df):
    print("task_2")
    print("\n")

    unique_cities = df["City"].dropna().unique()
    print("Уникальные города:")
    print(unique_cities)

    print("\n\n\n")


# Задание 3: Количество товаров в каждой категории
@ensure_dataset_loaded(required_columns=["Category", "ProductID"])
def task_3(df):
    print("task_3")
    print("\n")

    product_count_per_category = df.groupby("Category")["ProductID"].count()
    print("Количество товаров в каждой категории:")
    print(product_count_per_category)

    print("\n\n\n")


# Задание 4: Среднее количество единиц товара на складе по категориям
@ensure_dataset_loaded(required_columns=["Category", "UnitsInStock"])
def task_4(df):
    print("task_4")
    print("\n")

    average_units_in_stock = df.groupby("Category")["UnitsInStock"].mean()
    print("Среднее количество единиц товара на складе по категориям:")
    print(average_units_in_stock)

    print("\n\n\n")


# Задание 5: Самый дорогой товар
@ensure_dataset_loaded(required_columns=["UnitPrice (Products)", "Product", "Category"])
def task_5(df):
    print("task_5")
    print("\n")

    max_price_row = df.loc[df["UnitPrice (Products)"].idxmax()]
    product_name = max_price_row["Product"]
    product_category = max_price_row["Category"]
    max_price = max_price_row["UnitPrice (Products)"]
    print(
        f"Самый дорогой товар: {product_name}, Категория: {product_category}, Цена: {max_price}"
    )

    print("\n\n\n")


# Задание 6: Суммарная прибыль по странам
@ensure_dataset_loaded(required_columns=["Country", "Profit"])
def task_6(df):
    print("task_6")
    print("\n")

    country_profit = df.groupby("Country")["Profit"].sum()
    print("Суммарная прибыль для каждой страны:")
    print(country_profit)

    print("\n\n\n")


# Задание 7: График продаж по странам и страна с наибольшими продажами
@ensure_dataset_loaded(required_columns=["Country", "Sales"])
def task_7(df):
    print("task_7")
    print("\n")

    country_sales = df.groupby("Country")["Sales"].sum()
    country_sales.plot(kind="bar")
    plt.title("Продажи по странам")
    plt.ylabel("Продажи")
    plt.xlabel("Страна")
    plt.show()

    max_sales_country = country_sales.idxmax()
    max_sales_value = country_sales.max()
    print(f"Страна с наибольшими продажами: {max_sales_country} {max_sales_value}")

    print("\n\n\n")


# Задание 8: Уникальные комбинации города и страны
@ensure_dataset_loaded(required_columns=["City and Country"])
def task_8(df):
    print("task_8")
    print("\n")

    unique_city_country = df["City and Country"].drop_duplicates()
    print("Уникальные комбинации города и страны:")
    print(unique_city_country)

    print("\n\n\n")


# Задание 9: Средняя стоимость продукта на единицу по категориям
@ensure_dataset_loaded(required_columns=["Category", "UnitCost"])
def task_9(df):
    print("task_9")
    print("\n")

    avg_cost_per_category = df.groupby("Category")["UnitCost"].mean()
    print("Средняя стоимость продукта на единицу по категориям:")
    print(avg_cost_per_category)

    print("\n\n\n")


# Задание 10: Топ-3 компании по наибольшей прибыли
@ensure_dataset_loaded(required_columns=["Customer Company", "Profit"])
def task_10(df):
    print("task_10")
    print("\n")

    top_companies = df.groupby("Customer Company")["Profit"].sum().nlargest(3)
    print("Топ-3 компании по наибольшей прибыли:")
    print(top_companies)

    print("\n\n\n")


# Задание 11: Фильтрация данных по количеству проданных товаров > 30
@ensure_dataset_loaded(required_columns=["Quantity"])
def task_11(df):
    print("task_11")
    print("\n")

    filtered_data = df.query("Quantity > 30")
    print("Записи с количеством проданных товаров больше 30:")
    print(filtered_data)

    print("\n\n\n")


# Задание 12: Топ-5 товаров по количеству заказанных единиц
@ensure_dataset_loaded(required_columns=["Product", "UnitsOnOrder"])
def task_12(df):
    print("task_12")
    print("\n")

    top_products = df.groupby("Product")["UnitsOnOrder"].sum().nlargest(5)
    print("Топ-5 товаров с наибольшим количеством заказанных единиц:")
    print(top_products)

    print("\n\n\n")


# Задание 13: Общее количество проданных товаров по каждому клиенту
@ensure_dataset_loaded(required_columns=["Customer", "Quantity"])
def task_13(df):
    print("task_13")
    print("\n")

    total_quantity_per_customer = df.groupby("Customer")["Quantity"].sum()
    print("Общее количество проданных товаров по каждому клиенту:")
    print(total_quantity_per_customer)

    print("\n\n\n")


# Задание 14: Количество уникальных категорий продуктов в каждом городе
@ensure_dataset_loaded(required_columns=["City", "Category"])
def task_14(df):
    print("task_14")
    print("\n")

    unique_categories_per_city = df.groupby("City")["Category"].nunique()
    print("Количество уникальных категорий продуктов в каждом городе:")
    print(unique_categories_per_city)


# Задание 15: Новый столбец с разницей между ценой продажи и себестоимостью
@ensure_dataset_loaded(required_columns=["UnitPrice", "UnitCost"])
def task_15(df):
    print("task_15")
    print("\n")

    df["Margin"] = df["UnitPrice"] - df["UnitCost"]
    max_margin_row = df.loc[df["Margin"].idxmax()]
    print("Запись с максимальной маржой:")
    print(max_margin_row)

    print("\n\n\n")


# Задание 16: График зависимости прибыли от количества проданных товаров для каждой категории
@ensure_dataset_loaded(required_columns=["Profit", "Quantity", "Category"])
def task_16(df):
    print("task_16")
    print("\n")

    for category, group in df.groupby("Category"):
        plt.scatter(group["Quantity"], group["Profit"], label=category)
    plt.title("Прибыль от количества проданных товаров по категориям")
    plt.xlabel("Количество проданных товаров")
    plt.ylabel("Прибыль")
    plt.legend()
    plt.show()

    print("\n\n\n")


# Задание 17: Средняя и медианная стоимость товаров для каждого клиента
@ensure_dataset_loaded(required_columns=["Customer", "UnitPrice (Products)"])
def task_17(df):
    print("task_17")
    print("\n")

    customer_stats = df.groupby("Customer")["UnitPrice (Products)"].agg(
        ["mean", "median"]
    )

    max_average_customer = customer_stats["mean"].idxmax()
    max_average_value = customer_stats["mean"].max()

    print(
        f"Клиент с наибольшей средней стоимостью: {max_average_customer} ({max_average_value:.2f})"
    )

    print("\nСредние и медианные стоимости для каждого клиента:")
    print(customer_stats)

    print("\n\n\n")


# Задание 18: Продажи по месяцам и график временного ряда
@ensure_dataset_loaded(required_columns=["Sales", "OrderDate"])
def task_18(df):
    print("task_18")
    print("\n")

    df["Month"] = df["OrderDate"].dt.to_period("M")
    sales_by_month = df.groupby("Month")["Sales"].sum()
    sales_by_month.plot()
    plt.title("Продажи по месяцам")
    plt.xlabel("Месяц")
    plt.ylabel("Продажи")
    plt.show()

    print("\n\n\n")


# Задание 19: Определите, какая компания работает с наибольшим количеством различных клиентов.
@ensure_dataset_loaded(required_columns=["Customer", "Customer Company"])
def task_19(df):
    print("task_19")
    print("\n")

    company_customer_counts = (
        df.groupby("Customer Company")["Customer"]
        .nunique()
        .reset_index()
        .rename(columns={"Customer": "UniqueCustomerCount"})
    )
    max_customer_company = company_customer_counts.loc[
        company_customer_counts["UniqueCustomerCount"].idxmax()
    ]
    print(
        f"Компания с наибольшим количеством различных клиентов: {max_customer_company['Customer Company']}, "
        f"Количество уникальных клиентов: {max_customer_company['UniqueCustomerCount']}"
    )

    print("\n\n\n")


# Задание 20: Запись в Excel
@ensure_dataset_loaded()
def task_20(df):
    print("task_20")
    print("\n")

    df.to_excel("sales_data_analysis.xlsx", index=False)
    print("Данные записаны в 'sales_data_analysis.xlsx.'")

    print("\n\n\n")


# Задание 21: Построение сводной таблицы (pivot table)
@ensure_dataset_loaded(required_columns=["Category", "City", "Profit"])
def task_21(df):
    print("task_21")
    print("\n")

    pivot_table = df.pivot_table(
        values="Profit",
        index="Category",
        columns="City",
        aggfunc="sum",
        fill_value=0,
    )
    print("Сводная таблица (сумма прибыли по каждой категории и городу):")
    print(pivot_table)

    print("\n\n\n")


# Задание 22: Топ-3 города по наибольшим продажам за последние 6 месяцев.
@ensure_dataset_loaded(required_columns=["Sales", "OrderDate"])
def task_22(df):
    print("task_22")
    print("\n")

    df["OrderDate"] = pd.to_datetime(
        df["OrderDate"], errors="coerce"
    )  # Преобразование даты
    last_6_months = df[
        df["OrderDate"] >= (df["OrderDate"].max() - pd.DateOffset(months=6))
    ]
    top_cities = last_6_months.groupby("City")["Sales"].sum().nlargest(3)
    print("Топ-3 города по наибольшим продажам за последние 6 месяцев:")
    print(top_cities)

    print("\n\n\n")


# Задание 23: Новый столбец с превышением средней прибыли по категории
@ensure_dataset_loaded(required_columns=["Category", "Profit"])
def task_23(df):
    print("task_23")
    print("\n")

    average_profit = df.groupby("Category")["Profit"].mean()
    df["Profit_Exceed_Avg"] = df["Profit"] - df["Category"].map(average_profit)
    top_5_exceeding = df.nlargest(5, "Profit_Exceed_Avg")
    print("Топ-5 записей с наибольшим превышением средней прибыли:")
    print(top_5_exceeding)

    print("\n\n\n")
