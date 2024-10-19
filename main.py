from dataset_utils import load_dataset
import tasks

if __name__ == "__main__":
    file_path = "dataset.xlsx"
    df = load_dataset(file_path)

    if df is not None:
        task_list = [
            tasks.task_1,
            tasks.task_2,
            tasks.task_3,
            tasks.task_4,
            tasks.task_5,
            tasks.task_6,
            tasks.task_7,
            tasks.task_8,
            tasks.task_9,
            tasks.task_10,
            tasks.task_11,
            tasks.task_12,
            tasks.task_13,
            tasks.task_14,
            tasks.task_15,
            tasks.task_16,
            tasks.task_17,
            tasks.task_18,
            tasks.task_19,
            tasks.task_20,
            tasks.task_21,
            tasks.task_22,
            tasks.task_23,
        ]

        for task in task_list:
            task(df)
    else:
        print("Не удалось загрузить датасет.")
