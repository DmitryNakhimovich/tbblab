// Nakhimovich omp lab #1
// BWT transform algorithm 

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <fstream>
#include <string>
#include "tbb/task_scheduler_init.h"
#include "tbb/parallel_reduce.h"
#include "tbb/blocked_range.h"
#include "tbb/tick_count.h"
#include "tbb/parallel_for.h"

using namespace std;
using namespace tbb;

#define uint unsigned int 

// подсчет кол-ва символов в числе
int digits(const int n) {

	if (n < 100000) {
		if (n < 100) {
			if (n < 10) {
				return 1;
			}
			else {
				return 2;
			}
		}
		else {
			if (n < 1000) {
				return 3;
			}
			else {
				if (n < 10000) {
					return 4;
				}
				else {
					return 5;
				}
			}
		}
	}
	return 0;
}

// сравнение для qsort
int compare(const void * a, const void * b)
{
	if (*(string*)a <  *(string*)b) return -1;
	if (*(string*)a == *(string*)b) return 0;
	if (*(string*)a >  *(string*)b) return 1;
}

// BWT преобразование (возвращает преобразованную строку)
string bwt_encode(string src, uint size, uint& key) {

	uint str_no;
	string dst;
	string* trans_matrix = new string[size];

	// Исполняем циклические сдвиги
	for (uint i = 0; i < size; i++) {
		trans_matrix[i] += src.substr(i, size - i);
		trans_matrix[i] += src.substr(0, i);
	}

	// Сортировка сдвигов  
	qsort(trans_matrix, size, sizeof(string), compare);

	// Сбор результата
	for (uint i = 0; i < size; i++) {
		dst += trans_matrix[i][size - 1]; //Все последние элементы

		if (trans_matrix[i] == src)
			str_no = i;
	}
	key = str_no;

	delete[] trans_matrix;

	return dst;
}

// RLE сжатие (возвращает число символов в сжатом тексте)
uint rle_encode(string* src, uint* keys, uint blocks, ofstream& out) {

	uint size = 0;
	char bt;
	short bt_num = 1;

	for (uint block = 0; block < blocks; block++) {
		out << "¶";
		size++;
		out << keys[block];
		size += digits(keys[block]);
		out << "·";
		size++;
		for (uint i = 0; i < src[block].length(); i++) {
			bt = src[block][i];
			if (i == src[block].length() - 1) {
				if (bt_num == 1) {
					out << bt;
					size++;
					bt_num = 0;
				}
				else {
					out << bt << "µ" << bt_num << "·";
					size += 3 + digits(bt_num);
					bt_num = 0;
				}
				bt_num++;
			}
			else {
				if (bt != src[block][i + 1]) {
					if (bt_num == 1) {
						out << bt;
						size++;
						bt_num = 0;
					}
					else {
						out << bt << "µ" << bt_num << "·";
						size += 3 + digits(bt_num);
						bt_num = 0;
					}
				}
				bt_num++;
			}
		}
	}
	return size;
}

// RLE декодирование (возвращает число символов в обратном тексте)
uint rle_decode(string src, string* text, uint* keys, uint text_size) {

	uint size = 0;
	char bt = '¶';
	string bt_num;
	uint block = 0;
	uint i = 0;
	char bt_prev;

	while (i < text_size) {
		bt_prev = bt;
		bt = src[i];
		if (bt == '¶') {
			i++;
			bt_num.clear();
			while (src[i] != '·') {
				bt_num += src[i];
				i++;
			}
			i++;
			keys[block] = atoi(bt_num.c_str());
			bt_num.clear();
			block++;
		}
		else
			if (bt == 'µ') {
				i++;
				bt_num.clear();
				while (src[i] != '·') {
					bt_num += src[i];
					i++;
				}
				i++;
				for (uint j = 0; j < atoi(bt_num.c_str()) - 1; j++) {
					text[block - 1] += (bt_prev);
					size++;
				}
				bt_num.clear();
			}
			else {
				text[block - 1] += (bt);
				i++;
				size++;
			}
	}
	return size;
}

// BWT декодирование (возвращает преобразованный блок)
string bwt_decode(string src, uint size, uint key) {
	string* trans_matrix = new string[size];
	string* temp_sl = new string[size];

	string tmp;
	for (int i = 0; i < size; i++) {
		tmp += '0';
	}

	for (int i = 0; i < size; i++) {
		trans_matrix[i] += tmp;
		temp_sl[i] += tmp;
	}

	for (int i = 0; i<size; i++) {
		trans_matrix[i][size - 1] = src[i];
	}

	for (int I = 0; I<(size - 1); I++) {
		for (int J = 0; J<(size); J++) {
			temp_sl[J][0] = trans_matrix[J][size - 1];
			for (int k = 0; k < I; k++)
				temp_sl[J][1 + k] = trans_matrix[J][k];
		}

		qsort(temp_sl, size, sizeof(string), compare);

		for (int J = 0; J < size; J++) {
			trans_matrix[J][I] = temp_sl[J][I];
		}
	}

	tmp = trans_matrix[key];

	delete[] trans_matrix;
	delete[] temp_sl;

	return tmp;
}

// Функтор класс для кодирования
class Coder {
public:
	string c_text;
	string *c_res;
	uint *c_key;
	uint c_block_size;
	uint c_block_num;

	Coder(string _text, string* _res, uint* _key, uint _block_size) : 
		 c_text(_text), c_res(_res), c_key(_key), c_block_size(_block_size)
	{}

	/*Coder(const Coder& c, split) : 
		c_text(c.c_text), c_res(c.c_res), c_key(c.c_key), c_block_size(c.c_block_size)
	{}*/

	void operator()(const blocked_range<int>& r) const
	{
		for (int i = r.begin(); i < r.end(); i++) {
			c_res[i] = bwt_encode(c_text.substr(i * c_block_size, c_block_size), c_block_size, c_key[i]);		
		}
	}
};

// Функтор класс для декодирования 
class Decoder {
public:
	string c_text;
	string *c_res;
	uint *c_key;
	uint c_block_size;
	uint c_block_num;

	Decoder(string* _res, uint* _key) :
			c_res(_res), c_key(_key)
	{}
	
	void operator()(const blocked_range<int>& r) const
	{
		for (int i = r.begin(); i < r.end(); i++) {
			c_res[i] = bwt_decode(c_res[i], c_res[i].length(), c_key[i]);
		}
	}
};

// Сравнение двух файлов
bool FileEqual(string file1, string file2) {
	ifstream in1, in2;
	string iter1, iter2;

	in1.open(file1.c_str());
	in2.open(file2.c_str());

	if (in1 && in2) {
		while (in1.good() || in2.good()) {
			getline(in1, iter1);
			getline(in2, iter2);
			if (iter1 != iter2)
			{
				in1.close();
				in2.close();
				return false;
			}
		}		
		in1.close();
		in2.close();		
		return true;
	}
	else {
		cout << "error to open text!" << endl;
		in1.close();
		in2.close();
		return false;
	}

	return false;
}

int main(int argc, char* argv[])
{
	//глобальные переменные
	ifstream text_in;//файловый поток на чтение
	ofstream text_out;//файловый поток на запись
	string text_array;//первоначальный текст
	uint iter = 0, text_size = 0, res_size = 0, block_size = 16, block_num = 0;
	string text_path = "1.txt";//путь к файлу на чтение
	string text_res = "out.txt";//путь к файлу результата
	string text_start = "start.txt";//путь к файлу исходный после преобразований 
	string text_first = "1.txt";//путь к исходному файлу 
	char iter_char;
	tick_count t1, t2;//таймер
	bool block_flag = false;
	string* bwt_res;//массив bwt блоков после преобразования
	uint* bwt_key;//массив ключей для каждого блока bwt преобразования
	uint threads_num = 1;//кол-во потоков
	char prog_type = 'c';//тип прогр., c - code \ d - decode
	bool file_compare = false;//сравнение исходного и конечного файла

	// инициализация параметров запуска
	cout << "Enter file path to open, file path to first text, type (c/d), threads num: " << endl;
	cin >> text_path;
	cin >> text_first;
	cin >> prog_type;
	cin >> threads_num;

	task_scheduler_init init(threads_num);//задание числа потоков 

	//считывание текста из файла
	text_in.open(text_path.c_str(), ios::in);
	if (text_in) {
		while ((iter_char = text_in.get()) != EOF) {
			text_array += iter_char;
			iter++;
		}
		text_in.close();
		text_size = iter;
		iter = 0;
	}
	else {
		cout << "error to open text!" << endl;
		text_in.close();
		return 0;
	}

	//проверка на длину 
	if (text_size > block_size * threads_num)
		block_flag = true;
	else
	{
		block_flag = false;
		threads_num = 1;
		cout << "short text, running with 1 thread!" << endl;
	}

	// ===============================================================================
	// кодирование если 'c' 
	if (prog_type == 'c') {

		// таймер старт
		t1 = tick_count::now();

		//bwt преобразование		
		if (block_flag) {
			uint chunk = text_size / block_size;
			uint rem = text_size % block_size;
			bwt_res = new string[chunk + 1];
			bwt_key = new uint[chunk + 1];

			/*for (int i = 0; i < chunk; i++) {
				bwt_res[i] = bwt_encode(text_array.substr(i * block_size, block_size), block_size, bwt_key[i]);
				block_num++;
			}*/

			// bwt кодирование 
			// Запуск параллельного алгоритма for	
			tbb::parallel_for(tbb::blocked_range<int>(0, chunk), Coder(text_array, bwt_res, bwt_key, block_size));
			bwt_res[chunk] = bwt_encode(text_array.substr(chunk * block_size, rem), rem, bwt_key[chunk]);

			/*for (int i = 0; i < chunk + 1; i++) {
				cout << bwt_res[i];
			}*/

			if (rem)
				block_num = chunk + 1;
			else
				block_num = chunk;
		}
		else {
			bwt_res = new string[1];
			bwt_key = new uint[1];
			bwt_res[0] = bwt_encode(text_array, text_size, bwt_key[0]);
			res_size = bwt_res[0].length();
			block_num = 1;
			cout << "_____________________________________" << endl << bwt_res[0] << endl;
		}

		// таймер стоп
		t2 = tick_count::now();

		//вывод в файл bwt текста с rle
		text_out.open(text_res.c_str(), ios::out);
		if (text_out) {
			res_size = rle_encode(bwt_res, bwt_key, block_num, text_out);
			text_out.close();
		}
		else {
			cout << "error to open result file!" << endl;
			text_out.close();

		}
	} 

	// ==============================================================================
	// если введено 'd'
	else {		
		
		// таймер старт
		t1 = tick_count::now();

		if (block_flag) {
			int chunk = text_size / block_size;
			int rem = text_size % block_size;
			bwt_res = new string[chunk + 1];
			bwt_key = new uint[chunk + 1];

			//rle декодирование 
			res_size = rle_decode(text_array, bwt_res, bwt_key, text_size);

			chunk = res_size / block_size;
			if (res_size % block_size)
				block_num = chunk + 1;
			else
				block_num = chunk;
						
			// bwt декодирование 
			// Запуск параллельного алгоритма for	
			tbb::parallel_for(tbb::blocked_range<int>(0, block_num), Decoder(bwt_res, bwt_key));
				
		}
		else {
			bwt_res = new string[1];
			bwt_key = new uint[1];

			block_num = 1;

			//rle декодирование 
			res_size = rle_decode(text_array, bwt_res, bwt_key, text_size);
			
			//bwt декодирование 
			bwt_res[0] = bwt_decode(bwt_res[0], bwt_res[0].length(), bwt_key[0]);
			
			cout << bwt_res[0];
						
		}

		// таймер стоп
		t2 = tick_count::now();

		// вывод исходного текста в файл 
		text_out.open(text_start.c_str(), ios::out);
		if (text_out) {
			for (uint i = 0; i < block_num; i++) {
				text_out << bwt_res[i];
			}
			text_out.close();
		}
		else {
			cout << "error to open start file!" << endl;
			text_out.close();

		}
	}

	if (prog_type != 'c')
		file_compare = FileEqual(text_first, text_start);

	//вывод результата
	cout << endl;
	cout << "text length: " << text_size
		 << ", res length: " << res_size
		 << ", blocks: " << block_num
		 << ", file compare: " << file_compare
		 << endl;
	cout << "time elapsed for BWT: " << (t2 - t1).seconds() << endl;
		
	//очистка памяти
	delete[] bwt_res;
	delete[] bwt_key;

	system("Pause");
	return 0;
}