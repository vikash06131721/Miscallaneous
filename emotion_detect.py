
"""
Pre-defined model built on Neural networks to detect emotions in sentences,
For running, download the model "emotion_detector", and place it in the same directory.
"""


import keras
from keras.models import load_model
from keras.preprocessing.sequence import pad_sequences
from keras.preprocessing.text import Tokenizer
import pandas as pd
import numpy as np

class emotion_detectr(object):

	def __init__(self,sentence):
		self.sentence=sentence

	def category(self,s):
	    if s==0:
	        return 'shame'
	    elif s==1:
	        return 'sadness'
	    elif s==2:
	        return 'fear'
	    elif s==3:
	        return 'anger'
	    elif s==4:
	        return 'guilt'
	    elif s==5:
	        return 'joy'
	    elif s==6:
	        return 'disgust'


	def predict_(self):
		s=self.sentence
		s_="I was devastated after the accident"
		s_df= pd.DataFrame(columns=['data'])
		# s_array=s.astype('object')
		s_list=[]
		s_list.append(s)
		s_list.append(s_)
		s_df['data']=s_list
		#Apply Simple Keras
		num_words=1000
		tokenizer= Tokenizer(num_words=num_words)
		tokenizer.fit_on_texts(s_df['data'].values)
		# Pad the data 
		X = tokenizer.texts_to_sequences(s_df['data'].values)
		X = pad_sequences(X, maxlen=1000)
		model= load_model('emotion_detector')
		pred_= model.predict_classes(X[0].reshape((1,1000)))
		print self.category(pred_[0])

if __name__ == '__main__':
	emot_= emotion_detectr("I feel disgust at such reports")
	emot_.predict_()








				


