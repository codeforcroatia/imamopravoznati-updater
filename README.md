# imamopravoznati-updater
IPZ Updater is a script that compares TJV Parser dataset against IPZ authority dataset and updates changes to imamopravoznati.org

This IPZ updater Python script is made by [SelectSoft](https://github.com/SelectSoft).

## Installation instructions

**IPZ updater** can be executed locally on machine:

I run Python version 3.7.3. You also have to add pandas by using pip or conda like:

`pip install pandas` (if you are using pip)

or

`conda install pandas` (if you are using Conda)

and you also have to change `authority-export.csv` file path according to your machine.

Default dataset path is /Data:
- `authority-export.csv` - source file from imamopravoznati.org
- `NewData.csv` - result of a script processing
