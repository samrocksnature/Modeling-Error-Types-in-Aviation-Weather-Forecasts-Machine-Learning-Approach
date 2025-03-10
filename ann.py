# -*- coding: utf-8 -*-
"""ANN

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1YORawpkFSYOwkIc0lWsaa3vmpDYQfN6a
"""

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import Dense
from keras.callbacks import EarlyStopping
from keras.optimizers import Adam
from keras.utils import to_categorical
from sklearn.metrics import classification_report, confusion_matrix
import tensorflow as tf

# Load the data
data = pd.read_csv('/content/merged_metar_taf.csv')

# Define features and target variables
features = ['Ceiling_TAF (ft)', 'Visibility_TAF (km)', 'Altitude (ft)', 'Latitude (degrees)', 'Longitude (degrees)', 'Distance (km)',
            'Koppen Climate Classification']
target = 'Error_type'

# Fix non-numeric issues
data['Visibility_METAR (km)'] = data['Visibility_METAR (km)'].replace({'10+': 10, '6+': 6}, regex=True).astype(float)
data['Visibility_TAF (km)'] = data['Visibility_TAF (km)'].replace({'6+': 6}, regex=True).astype(float)
data['Longitude (degrees)'] = data['Longitude (degrees)'].str.extract(r'([-0-9.]+)').astype(float)
data['Weather_TAF'] = data['Weather_TAF'].astype('category').cat.codes
data['Koppen Climate Classification'] = data['Koppen Climate Classification'].astype('category').cat.codes
error_mapping = {
    'Correct - No error (0)': 0,
    'Fail to Detect - Type II (2) error': 2,
    'False Alarm - Type I (1) error': 1
}
data['Error_type'] = data['Error_type'].map(error_mapping)
data[features + [target]] = data[features + [target]].apply(pd.to_numeric, errors='coerce')

# Ensure the dataset is not empty
data = data.dropna()
if data.empty:
    raise ValueError("The dataset is empty after filtering. Please check the input data.")

# Check the unique classes in the target
unique_classes = data['Error_type'].unique()
print(f"Unique classes in the target: {unique_classes}")

# Ensure all three classes are present
if len(unique_classes) < 3:
    raise ValueError("Not all classes are represented in the dataset. Please check the data.")

# Scaling features to [0,1]
scaler = MinMaxScaler()
scaled_features = scaler.fit_transform(data[features])
X = scaled_features
y = data[target].values

# One-hot encoding the target
y_onehot = to_categorical(y, num_classes=3)  # Ensure 3 classes for one-hot encoding

# Splitting the dataset into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y_onehot, test_size=0.20, random_state=116008)

# Building the ANN
tf.random.set_seed(454545)  # For reproducibility
multiclassifier = Sequential()

# Input and hidden layers
multiclassifier.add(Dense(units=64, activation='relu', input_dim=X_train.shape[1]))
multiclassifier.add(Dense(units=32, activation='relu'))

# Output layer with softmax activation (3 units for 3 classes)
multiclassifier.add(Dense(units=3, activation='softmax'))

# Compile the model
optimizer = Adam(learning_rate=0.001)
multiclassifier.compile(loss='categorical_crossentropy', optimizer=optimizer, metrics=['accuracy'])

# Early stopping
early_stopping = EarlyStopping(monitor='loss', patience=10, restore_best_weights=True)

# Training the model
multiclassifier.fit(X_train, y_train, epochs=200, batch_size=32, verbose=1, callbacks=[early_stopping])

# Predicting probabilities for the test set
pred_prob = multiclassifier.predict(X_test)

# Determine the predicted class
y_pred = np.argmax(pred_prob, axis=1)
y_true = np.argmax(y_test, axis=1)

# Computing prediction accuracy
accuracy = np.mean(y_pred == y_true)
print(f'Accuracy = {round(accuracy, 4)}')

# Display confusion matrix and classification report
print("Confusion Matrix:\n", confusion_matrix(y_true, y_pred))
print("\nClassification Report:\n", classification_report(y_true, y_pred, digits=4))