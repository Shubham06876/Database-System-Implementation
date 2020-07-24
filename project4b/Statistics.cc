#include "Statistics.h"
#include <climits>
#include <stdio.h>
#include <cstring>
#include <iostream>
#include <stdlib.h> 

Statistics::Statistics(){}

Statistics::Statistics(Statistics &copyMe)
{
	for (map<string,relSchema>::iterator it = copyMe.relMap.begin(); it != copyMe.relMap.end(); ++it){
		string relN = it->first;
		relSchema rS;
		rS.numTuples = it->second.numTuples;
		rS.numRel = it->second.numRel;
		
		for (map<string, int>::iterator it_a = it->second.attMap.begin(); it_a!=it->second.attMap.end(); ++it_a){
			string attN = it_a->first;
			int n = it_a->second;
			rS.attMap.insert(pair<string, int>(attN, n));
		}
		relMap.insert(pair<string, relSchema>(relN, rS));
		rS.attMap.clear();
	}
}

Statistics::~Statistics()
{
	for (map<string,relSchema>::iterator it = relMap.begin(); it != relMap.end(); ++it){
		it->second.attMap.clear();
	}
	relMap.clear();
}


//This operation adds another base relation into the structure. The parameter set tells the
//statistics object what the name and size of the new relation is (size is given in terms of
//the number of tuples)
bool Statistics::AddRel(char *relName, int numTuples)
{
	if(relName == NULL){
		return false;
	}
	string relN(relName);
	map<string,relSchema>::iterator t_iter = relMap.find(relN);

	
	//update numTuples if relName is in relMap
	if(t_iter != relMap.end()){
		t_iter->second.numTuples = numTuples;
		return true;
	}
	//we need to insert new entry of relSchema in relMap if relName is not in relMap
	else{
		relSchema relS;
		
		relS.numTuples = numTuples;
		relS.numRel = 1;

		relMap.insert(pair<string, relSchema>(relN,relS));
		return true;
	}
}


//This operation adds an attribute to one of the base relations in the structure. The
//parameter set tells the Statistics object what the name of the attribute is, what
//relation the attribute is attached to, and the number of distinct values that the relation has
//for that particular attribute. If numDistincts is initially passed in as a –1, then the
//number of distincts is assumed to be equal to the number of tuples in the associated
//relation.

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{

	string relN(relName);
	string attN(attName);
	map<string,relSchema>::iterator t_iter = relMap.find(relN);
	if(t_iter == relMap.end()){
		cerr << "Error: relName not found when adding an attribute!";
		return;
	}
	map<string, int>::iterator a_iter = t_iter->second.attMap.find(attN);
	//attribute is already in attMap
	
	if(a_iter != t_iter->second.attMap.end()){
		if (numDistincts == -1)
			numDistincts = t_iter->second.numTuples;
		a_iter->second = numDistincts;
	}
	//we need to insert a new attribute if that attribute is not in attMap
	else{
		

		if (numDistincts == -1){
			numDistincts = t_iter->second.numTuples;
		}
		t_iter->second.attMap.insert(pair<string, int>(attN, numDistincts));
	}
}


//This operation produces a copy of the relation (including all of its attributes and all of its
//statistics) and stores it under the new name.

void Statistics::CopyRel(char *oldName, char *newName)
{
	cout<<"\n"<<oldName<<" : "<<newName;
	
	string oldN(oldName);
	string newN(newName);
	map<string,relSchema>::iterator t_iter = relMap.find(oldN);
	if(t_iter == relMap.end()){
		cerr << "Error: relName not found when adding a attribute!";
		return;
	}
	
	if(relMap.find(newN) != relMap.end()){
		return;
	}
	

	relSchema relS;
	relS.numTuples = t_iter->second.numTuples;
	relS.numRel = t_iter->second.numRel;
	for (map<string, int>::iterator a_iter = t_iter->second.attMap.begin(); a_iter != t_iter->second.attMap.end(); ++a_iter){
		char * new_attribute = new char[100];
		sprintf(new_attribute, "%s.%s", newName, a_iter->first.c_str());
		string temp(new_attribute);
		relS.attMap.insert(pair<string,int>(temp ,a_iter->second));
		delete new_attribute;
	}
	relMap.insert(pair<string,relSchema>(newN,relS));
}
	
//Statistics object also has the ability to read from text file
void Statistics::Read(char *fromWhere)
{
	relMap.clear();
	FILE* file_to_read;
	file_to_read = fopen(fromWhere, "r");
	if(file_to_read == NULL){
		file_to_read = fopen(fromWhere, "w");
		fprintf(file_to_read, "end\n");
		fclose(file_to_read);
		return;
	}
	char read_str[100], read_char[100];
	
	fscanf(file_to_read, "%s", read_str);
	int nbr;

	while(strcmp(read_str, "end")){
		if(strcmp(read_str, "rel")){
				
		}
		else{
			relSchema relS;
			relS.numRel = 0;
			fscanf(file_to_read, "%s", read_str);
			string relstr(read_str);
			strcpy(read_char, relstr.c_str());
			fscanf(file_to_read, "%s", read_str);
			relS.numTuples = atoi(read_str);
			fscanf(file_to_read, "%s", read_str);
			fscanf(file_to_read, "%s", read_str);
			
			while(strcmp(read_str, "rel") && strcmp(read_str, "end")){
				string attstr(read_str);				
				fscanf(file_to_read, "%s", read_str);
				nbr = atoi(read_str);
				relS.attMap.insert(pair<string, int>(attstr, nbr));
				fscanf(file_to_read, "%s", read_str);
			}
			relMap.insert(pair<string, relSchema>(relstr, relS));			
			relS.numRel++;
		}
	}
	fclose(file_to_read);
}


//Statistics object also has the ability to read from text file
void Statistics::Write(char *toWhere)
{
	FILE* file_to_write;
	file_to_write = fopen(toWhere, "w");
	for(map<string,relSchema>::iterator r_iter = relMap.begin(); r_iter != relMap.end(); ++r_iter){
		char * r_temp = new char[r_iter->first.length()+1];
		strcpy(r_temp, r_iter->first.c_str());
		fprintf(file_to_write, "rel\n%s\n", r_temp);
		fprintf(file_to_write, "%d\nattrs\n", r_iter->second.numTuples);
		for(map<string,int>::iterator a_iter = r_iter->second.attMap.begin(); a_iter != r_iter->second.attMap.end(); ++a_iter){
			char * a_temp = new char[a_iter->first.length()+1];
			strcpy(a_temp, a_iter->first.c_str());
			fprintf(file_to_write, "%s\n%d\n", a_temp, a_iter->second);
			delete a_temp;
		}
		delete r_temp;
	}
	fprintf(file_to_write, "end\n");
	fclose(file_to_write);
}


//The Apply operation uses the statistics stored by the Statistics class to simulate a join of all of
//the relations listed in the relNames parameter. This join is performed using the predicates listed in 
//the parameter parseTree. Join can be understood as when two or more relations are within the same 
//subset, it means that they have been “joined” and they do not exist independently anymore. This 
//function doesn’t actually perform join as join has been implemented in relOps. 

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
	Estimate(parseTree, relNames, numToJoin);
	struct AndList * r_andl = parseTree;
	struct OrList * r_orl;
	while (r_andl != NULL){
		if (r_andl->left != NULL){
			r_orl = r_andl->left;
			while(r_orl != NULL){
				
				if (r_orl->left->left->code == NAME && r_orl->left->right->code == NAME){
					map<string, int>::iterator iterator_attr[2];
					map<string, relSchema>::iterator iterator_rel[2];
					string joinAtt1(r_orl->left->left->value);
					string joinAtt2(r_orl->left->right->value);
					for (map<string, relSchema>::iterator r_iter = relMap.begin(); r_iter != relMap.end(); ++r_iter){
						iterator_attr[0] = r_iter->second.attMap.find(joinAtt1);

						if(iterator_attr[0] != r_iter->second.attMap.end()){
							iterator_rel[0] = r_iter;
							break;
						}
					}
					for (map<string, relSchema>::iterator r_iter = relMap.begin(); r_iter != relMap.end(); ++r_iter){
						iterator_attr[1] = r_iter->second.attMap.find(joinAtt2);

						if(iterator_attr[1] != r_iter->second.attMap.end()){
							iterator_rel[1] = r_iter;
							break;
						}
					}
					relSchema relS_join;
					char * relS_name = new char[200];
					sprintf(relS_name, "%s|%s", iterator_rel[0]->first.c_str(), iterator_rel[1]->first.c_str());
					string joinNamestr(relS_name);
					relS_join.numTuples = tempRes;
					relS_join.numRel = numToJoin;
					int z = 0;
					while(z < 2){
						for (map<string, int>::iterator a_iter = iterator_rel[z]->second.attMap.begin(); a_iter != iterator_rel[z]->second.attMap.end(); ++a_iter){
							relS_join.attMap.insert(*a_iter);
						}
						relMap.erase(iterator_rel[z]);
						z++;
					}
					relMap.insert(pair<string, relSchema>(joinNamestr, relS_join));
				}
				else{
					string seleAtt(r_orl->left->left->value);
					map<string, int>::iterator iterator_attr;
					map<string, relSchema>::iterator iterator_rel;
					for (map<string, relSchema>::iterator r_iter = relMap.begin(); r_iter != relMap.end(); ++r_iter){
						iterator_attr = r_iter->second.attMap.find(seleAtt);
						if(iterator_attr != r_iter->second.attMap.end()){
							iterator_rel = r_iter;
							break;
						}
					}
					iterator_rel->second.numTuples = tempRes;					
				}
				r_orl = r_orl->rightOr;
			}
		}
		r_andl = r_andl->rightAnd;
	}
}

//This operation is exactly like Apply, except that it does not actually change the state
//of the Statistics object. Instead, it computes the number of tuples that would
//result from a join over the relations in relNames, and returns this to the caller.

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
	struct AndList * r_andl = parseTree;
	struct OrList * r_orl;
	double return_val = 0.0, frac_value = 1.0;
	int flag = 0;

	while (r_andl != NULL){
		if (r_andl->left != NULL){
			r_orl = r_andl->left;
			double frac_or = 0.0;
			map<string, int>::iterator l_attribute;
			while(r_orl != NULL){
				
				if (r_orl->left->left->code == NAME && r_orl->left->right->code == NAME){
					map<string, int>::iterator iterator_attr[2];
					map<string, relSchema>::iterator iterator_rel[2];
					string joinAtt1(r_orl->left->left->value);
					string joinAtt2(r_orl->left->right->value);

					for (map<string, relSchema>::iterator r_iter = relMap.begin(); r_iter!=relMap.end(); ++r_iter){
						iterator_attr[0] = r_iter->second.attMap.find(joinAtt1);
						if(iterator_attr[0] != r_iter->second.attMap.end()){
							iterator_rel[0] = r_iter;
							break;
						}
					}
					for (map<string, relSchema>::iterator r_iter = relMap.begin(); r_iter!=relMap.end(); ++r_iter){
						iterator_attr[1] = r_iter->second.attMap.find(joinAtt2);
						if(iterator_attr[1] != r_iter->second.attMap.end()){
							iterator_rel[1] = r_iter;
							break;
						}
					}
					
					double max;
					if (iterator_attr[0]->second < iterator_attr[1]->second)
					{
						max = (double)iterator_attr[1]->second;		
					}
					else
					{		
						max = (double)iterator_attr[0]->second;
					}
					if (flag != 0){
						return_val = return_val*(double)iterator_rel[1]->second.numTuples/max;
					}
					else{
						return_val = (double)iterator_rel[0]->second.numTuples*(double)iterator_rel[1]->second.numTuples/max;
					}

					flag = 1;
				}
				else{
					string seleAtt(r_orl->left->left->value);
					map<string, int>::iterator iterator_attr;
					map<string, relSchema>::iterator iterator_rel;
					for (map<string, relSchema>::iterator r_iter = relMap.begin(); r_iter!=relMap.end(); ++r_iter){
						iterator_attr = r_iter->second.attMap.find(seleAtt);
						if(iterator_attr != r_iter->second.attMap.end()){
							iterator_rel = r_iter;
							break;
						}
					}
					if (return_val == 0.0){
						return_val = ((double)iterator_rel->second.numTuples);
					}
					double tempFrac;
					if(r_orl->left->code == EQUALS){
						tempFrac = 1.0 / iterator_attr->second;
					}
					else{
						tempFrac = 1.0 / 3.0;
					}
					if(l_attribute == iterator_attr){
						frac_or += tempFrac;
						
					}
					else{
						frac_or = tempFrac+frac_or-(tempFrac*frac_or);
					}
					l_attribute = iterator_attr;
				}
				r_orl = r_orl->rightOr;
			}
			if (frac_or != 0.0){
				frac_value = frac_value*frac_or;
			}
		}
		r_andl = r_andl->rightAnd;
	}
	return_val = return_val * frac_value;
	tempRes = return_val;
	return return_val;
}
