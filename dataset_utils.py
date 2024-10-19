import pandas as pd


# Функция загрузки датасета
def load_dataset(file_path):
    try:
        return pd.read_excel(file_path)
    except FileNotFoundError:
        print(f"Ошибка: Файл {file_path} не найден.")
        return None
    except Exception as e:
        print(f"Ошибка при загрузке файла: {e}")
        return None


# Декоратор для проверки наличия датасета и необходимых столбцов
def ensure_dataset_loaded(required_columns=None):
    def decorator(func):
        def wrapper(df, *args, **kwargs):
            if df is None or df.empty:
                print("Датасет не загружен или пуст.")
                return

            if required_columns:
                missing = [col for col in required_columns if col not in df.columns]
                if missing:
                    print(f"Не найдены столбцы: {', '.join(missing)}")
                    return

            return func(df, *args, **kwargs)

        return wrapper

    return decorator
