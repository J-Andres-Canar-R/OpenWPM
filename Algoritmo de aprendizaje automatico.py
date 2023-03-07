import os
import pandas as pd
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.metrics import accuracy_score
from sklearn.naive_bayes import MultinomialNB
from sklearn.model_selection import train_test_split
import re
import nltk
from nltk.corpus import stopwords

nltk.download('stopwords')

valid_path = "/home/jandres/Escritorio/Tesis/Politicas/Informacion valida"
invalid_path = "/home/jandres/Escritorio/Tesis/Politicas/Informacion no valida"
frames = []

# función para tokenizar, limpiar y eliminar stopwords, números y caracteres especiales
def preprocess_text(text):
    # Reemplazar saltos de línea por espacios en blanco
    text = text.replace('\n', ' ')
    # tokenizar el texto en palabras
    words = nltk.word_tokenize(text.lower())
    # eliminar números y caracteres especiales
    words = [word for word in words if re.match(r'^[a-záéíóúñ]+$', word)]
    # eliminar stopwords del español
    stop_words = set(stopwords.words('spanish'))
    words = [word for word in words if word not in stop_words]
    # unir las palabras en un solo texto
    clean_text = ' '.join(words)
    return clean_text

# leer archivos de la carpeta de información válida y crear un DataFrame con CLASS = 1
for filename in os.listdir(valid_path):
    if filename.endswith(".txt"):
        with open(os.path.join(valid_path, filename), 'r') as f:
            content = f.read()
            # preprocesar el texto
            clean_text = preprocess_text(content)
            # crear un nuevo DataFrame con una nueva columna "CONTENT", "CLASS" y "ENTITY"
            entity_name = os.path.splitext(filename)[0]  # obtener solo el nombre base del archivo
            df = pd.DataFrame({"ENTITY": [entity_name], "CONTENT": [clean_text], "CLASS": [1]})
            df = df[["ENTITY", "CONTENT", "CLASS"]]
            frames.append(df)

# leer archivos de la carpeta de información no válida y crear un DataFrame con CLASS = 0
for filename in os.listdir(invalid_path):
    if filename.endswith(".txt"):
        with open(os.path.join(invalid_path, filename), 'r') as f:
            content = f.read()
            # preprocesar el texto
            clean_text = preprocess_text(content)
            # crear un nuevo DataFrame con una nueva columna "CONTENT", "CLASS" y "ENTITY"
            entity_name = os.path.splitext(filename)[0]  # obtener solo el nombre base del archivo
            df = pd.DataFrame({"ENTITY": [entity_name], "CONTENT": [clean_text], "CLASS": [0]})
            df = df[["ENTITY", "CONTENT", "CLASS"]]
            frames.append(df)

# concatenar los DataFrames en un solo DataFrame
result = pd.concat(frames, ignore_index=True)


# extraer las columnas de interés y aplicar CountVectorizer
df_data = result[["CONTENT", "CLASS"]]
df_x = df_data['CONTENT']
df_y = df_data['CLASS']
# Crear el vectorizador y transformar el corpus
corpus = df_x
cv = CountVectorizer()
X = cv.fit_transform(corpus)

# Dividir el conjunto de datos en entrenamiento y prueba
X_train, X_test, y_train, y_test = train_test_split(X, df_y, test_size=0.30, random_state=50)

#print(sum(y_test))
#print(len(y_test))

# Entrenar el modelo y evaluar su precisión
clf = MultinomialNB()
clf.fit(X_train,y_train)
print("Accuracy of Model",clf.score(X_test,y_test)*100,"%")

# Evaluar el modelo con accuracy score
y_pred = clf.predict(X_test)
accuracy = accuracy_score(y_test, y_pred)
print("Accuracy of the model on the test set: {:.2f}%".format(accuracy*100))



# Vectorizar un nuevo documento y predecir su clase
path_privacy_policy = "/home/jandres/Escritorio/Tesis/Politicas/Informacion valida/aig.com.ec.txt"
#path_privacy_policy = "/home/jandres/Escritorio/Tesis/Politicas/Informacion no valida/ecuavisa.com.txt"

with open(path_privacy_policy, "r") as f:
    content = f.read()
    # preprocesar el texto
    clean_text = preprocess_text(content)

privacy_policy = [clean_text]
vect = cv.transform(privacy_policy).toarray()
prediction = clf.predict(vect)[0]
print("Class of privacy_policy:", prediction)