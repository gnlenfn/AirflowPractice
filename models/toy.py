from tensorflow import keras

imdb = keras.datasets.imdb
def data_load():
    """
    keras의 IMDB 데이터 불러오기
    """
    (X_train, y_train), (X_test, y_test) = imdb.load_data(num_words=10000)
    return X_train, X_test, y_train, y_test


def preprocess():
    # 몇 개의 인덱스는 사전에 정의되어있음
    word_index = imdb.get_word_index()
    word_index = {k: (v + 3) for k, v in word_index.items()}
    word_index["<PAD>"] = 0
    word_index["<START>"] = 1
    word_index["<UNK>"] = 2  # unknown
    word_index["<UNUSED>"] = 3
    # tensor 변환
    train_data, test_data, train_labels, test_labels = data_load()
    train_dataset = keras.preprocessing.sequence.pad_sequences(train_data,
                                                               value=word_index["<PAD>"],
                                                               padding='post',
                                                               maxlen=256)

    test_dataset = keras.preprocessing.sequence.pad_sequences(test_data,
                                                              value=word_index["<PAD>"],
                                                              padding='post',
                                                              maxlen=256)

    return train_dataset, test_dataset


# 입력 크기는 영화 리뷰 데이터셋에 적용된 어휘 사전의 크기입니다(10,000개의 단어)
def build_model():
    vocab_size = 10000

    m = keras.Sequential()
    m.add(keras.layers.Embedding(vocab_size, 16, input_shape=(None,)))
    m.add(keras.layers.GlobalAveragePooling1D())
    m.add(keras.layers.Dense(16, activation='relu'))
    m.add(keras.layers.Dense(1, activation='sigmoid'))
    m.compile(optimizer='adam',
                  loss='binary_crossentropy',
                  metrics=['accuracy'])
    return m


def model_training(model, train_dataset, train_labels):
    x_val = train_dataset[:10000]
    partial_x_train = train_dataset[10000:]

    y_val = train_labels[:10000]
    partial_y_train = train_labels[10000:]

    model.fit(partial_x_train,
              partial_y_train,
              epochs=40,
              batch_size=512,
              validation_data=(x_val, y_val),
              verbose=1)
    model.save('trained_model')


def evaluate_model(test_dataset, test_labels):
    loaded_model = keras.models.load_model('/Users/honginyoon/airflow/dags/models/trained_model')
    result = loaded_model.evaluate(test_dataset, test_labels, verbose=2)
    return result


if __name__ == "__main__":
    # train_data, test_data, train_label, test_label = data_load()
    _, _, train_label, test_label = data_load()
    train_data, test_data = preprocess()
    mymodel = build_model()
    model_training(mymodel, train_data, train_label)

    # print(evaluate_model(test_data, test_label))
