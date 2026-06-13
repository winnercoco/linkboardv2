from pathlib import Path
import pandas as pd
import shutil

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR/ "data"
FILE_NAME = "links.xlsx"
LINKS_PATH = DATA_DIR/ FILE_NAME
df = pd.read_excel(LINKS_PATH)
print("Reading file:",FILE_NAME,"\n")

def uniformity(selectedColumn):
    columnName = selectedColumn.upper()    #create printable column name
    printerString = f"========================{columnName}========================"
    print(printerString)
    print("Total rows:",len(df[selectedColumn]))

    words = (
        df[selectedColumn]
        .str.split(",")
        .explode()
        .str.strip()
        .drop_duplicates()
        .sort_values()
        .reset_index(drop=True)
    )

    print(f"Total count of unique {columnName} is: {len(words.tolist())}") #create a string template to get length
    print("=" * len(printerString)) #print the underline

    for word in words:
        print(word)     #display the actual words in a list format
    print("=" * len(printerString)) #print the underline

header = df.columns.drop(["main_link", "duration", "rate"]).tolist()

#chatgpt code
def print_centered_box(title, items):
    terminal_width = shutil.get_terminal_size().columns

    box_width = max(len(title), max(len(item) for item in items)) + 8

    border = "=" * box_width
    title_line = f"| {title.center(box_width - 4)} |"

    print(border.center(terminal_width))
    print(title_line.center(terminal_width))
    print(border.center(terminal_width))

    for item in items:
        line = f"| {item.ljust(box_width - 4)} |"
        print(line.center(terminal_width))

    print(border.center(terminal_width))

    return box_width


menu_items = [
    "1: Studio",
    "2: Stars",
    "3: Core Categories",
    "4: Categories",
    "5: Positions",
    "6: Language",
    "7: General Tags",
    "8: Website",
    "9: Exit"
]

while True:
    box_width = print_centered_box("Verify Uniformity of Data", menu_items)

    terminal_width = shutil.get_terminal_size().columns
    left_padding = (terminal_width - box_width) // 2

    choice = int(input(" " * left_padding + "Enter Choice: "))

    match choice:
        case 1 | 2 | 3 | 4 | 5 | 6 | 7 | 8:
            uniformity(header[choice - 1])
        case 9:
            print("Exiting.....")
            break
        case _:
            print("Invalid Choice")