#include "Statistics.h"
#include "Errors.h"
#include <climits>

#include <stdio.h>
#include <cstring>
#include <iostream>
#include <stdlib.h> 
 
Statistics::Statistics(){}




char* Statistics::SearchAttr(char * attrName){
	map<string, int>::iterator it;
	string attrStr(attrName);
	string rel;
	char * relName = new char[200];
	for (map<string, relInfo>::iterator it1 = mymap.begin(); it1 != mymap.end(); ++it1){
		it = it1->second.attrs.find(attrStr);
		if (it != it1->second.attrs.end()){
			rel = it1->first;
			break;
		}
	}
	return (char*)rel.c_str();
}


/*

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





*/




void Statistics::CopyRel(char *oldName, char *newName){
	string old(oldName);
	string newname(newName);
	map<string,relInfo>::iterator it = mymap.find(old);
	relInfo newRel;
	newRel.numTuples = it->second.numTuples;
	newRel.numRel = it->second.numRel;
	for (map<string,int>::iterator it2 = it->second.attrs.begin(); it2!=it->second.attrs.end(); ++it2){
		char * newatt = new char[200];
		sprintf(newatt, "%s.%s", newName, it2->first.c_str());
		string temp(newatt);
		newRel.attrs.insert(pair<string, int>(newatt, it2->second));
	}
	mymap.insert(pair<string, relInfo>(newname, newRel));
}

void Statistics::AddRel(char *relName, int numTuples){
	relInfo newRel;
	newRel.numTuples = numTuples;
	newRel.numRel = 1;
	string str(relName);
	mymap.insert(pair<string, relInfo>(str, newRel));
}
	
Statistics::Statistics(Statistics &copyMe){
	for (map<string,relInfo>::iterator it1 = copyMe.mymap.begin(); it1!=copyMe.mymap.end(); ++it1){
		string str1 = it1->first;
		relInfo relinfo;
		relinfo.numTuples = it1->second.numTuples;
		relinfo.numRel = it1->second.numRel;
		
		for (map<string, int>::iterator it2 = it1->second.attrs.begin(); it2!=it1->second.attrs.end(); ++it2){
			string temp_str = it2->first;
			int num = it2->second;
			relinfo.attrs.insert(pair<string, int>(temp_str, num));
		}
		mymap.insert(pair<string, relInfo>(str1, relinfo));
		relinfo.attrs.clear();
	}
}

void Statistics::Write(char *fromWhere){
	FILE* statfile;
	statfile = fopen(fromWhere, "w");
	for (map<string,relInfo>::iterator it1 = mymap.begin(); it1!=mymap.end(); ++it1){
		char * write = new char[it1->first.length()+1];
		strcpy(write, it1->first.c_str());
		fprintf(statfile, "rel\n%s\n", write);
		fprintf(statfile, "%d\nattrs\n", it1->second.numTuples);
		for (map<string,int>::iterator it2 = it1->second.attrs.begin(); it2!=it1->second.attrs.end(); ++it2){
			char * watt = new char[it2->first.length()+1];
			strcpy(watt, it2->first.c_str());
			fprintf(statfile,"%s\n%d\n", watt, it2->second);
		}
	}
	fprintf(statfile, "end\n");
	fclose(statfile);
}



void Statistics::AddAtt(char *relName, char *attName, int numDistincts){
	string rel(relName);
	string att(attName);
	map<string,relInfo>::iterator it = mymap.find(rel);
	if (numDistincts == -1)
		numDistincts = it->second.numTuples;
	it->second.attrs.insert(pair<string, int>(att, numDistincts));
}



/*


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






*/



void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin){
	struct AndList * andlist = parseTree;
	struct OrList * orlist;
	while (andlist != NULL){
		if (andlist->left != NULL){
			orlist = andlist->left;
			while(orlist != NULL){
				if (orlist->left->left->code == 3 && orlist->left->right->code == 3){//
					map<string, int>::iterator itAtt[2];
					map<string, relInfo>::iterator itRel[2];
					string joinAtt1(orlist->left->left->value);
					string joinAtt2(orlist->left->right->value);
					for (map<string, relInfo>::iterator iit = mymap.begin(); iit!=mymap.end(); ++iit){
						itAtt[0] = iit->second.attrs.find(joinAtt1);
						if(itAtt[0] != iit->second.attrs.end()){
							itRel[0] = iit;
							break;
						}
					}
					for (map<string, relInfo>::iterator iit = mymap.begin(); iit!=mymap.end(); ++iit){
						itAtt[1] = iit->second.attrs.find(joinAtt2);
						if(itAtt[1] != iit->second.attrs.end()){
							itRel[1] = iit;
							break;
						}
					}
					relInfo joinedRel;
					char * joinName = new char[200];
					sprintf(joinName, "%s|%s", itRel[0]->first.c_str(), itRel[1]->first.c_str());
					string joinNamestr(joinName);
					joinedRel.numTuples = tempRes;
					joinedRel.numRel = numToJoin;
					for(int i = 0; i < 2; i++){
						for (map<string, int>::iterator iit = itRel[i]->second.attrs.begin(); iit!=itRel[i]->second.attrs.end(); ++iit){
							joinedRel.attrs.insert(*iit);
						}
						mymap.erase(itRel[i]);
					}
					mymap.insert(pair<string, relInfo>(joinNamestr, joinedRel));
				}
				else{
					string seleAtt(orlist->left->left->value);
					map<string, int>::iterator itAtt;
					map<string, relInfo>::iterator itRel;
					for (map<string, relInfo>::iterator iit = mymap.begin(); iit!=mymap.end(); ++iit){
						itAtt = iit->second.attrs.find(seleAtt);
						if(itAtt != iit->second.attrs.end()){
							itRel = iit;
							break;
						}
					}
					itRel->second.numTuples = tempRes;
					
				}
				orlist = orlist->rightOr;
			}
		}
		andlist = andlist->rightAnd;
	}


}

void Statistics::Read(char *fromWhere){
  mymap.clear(); tempRes = 0.0;
	FILE* statfile;
	statfile = fopen(fromWhere, "r");
	if (statfile == NULL){
		statfile = fopen(fromWhere, "w");
		fprintf(statfile, "end\n");
		fclose(statfile);
		statfile = fopen(fromWhere, "r");
	}
	char fstr[200], relchar[200];
	int n;
	fscanf(statfile, "%s", fstr);
	while(strcmp(fstr, "end")){
		if(!strcmp(fstr, "rel")){
			relInfo relinfo;
			relinfo.numRel = 1;
			fscanf(statfile, "%s", fstr);
			string relstr(fstr);
			strcpy(relchar, relstr.c_str());
			fscanf(statfile, "%s", fstr);
			relinfo.numTuples = atoi(fstr);
			fscanf(statfile, "%s", fstr);
			fscanf(statfile, "%s", fstr);
			
			while(strcmp(fstr, "rel") && strcmp(fstr, "end")){
				string attstr(fstr);				
				fscanf(statfile, "%s", fstr);
				n = atoi(fstr);
				relinfo.attrs.insert(pair<string, int>(attstr, n));
				fscanf(statfile, "%s", fstr);
			}
			mymap.insert(pair<string, relInfo>(relstr, relinfo));				
		}
	}
	fclose(statfile);
}


/*

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





*/

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin){
	struct AndList * andlist = parseTree;
	struct OrList * orlist;
	double result = 0.0, fraction = 1.0;
	int state = 0;
        if (andlist == NULL) {
          if (numToJoin>1) return -1;
          FATALIF (mymap.find(relNames[0]) == mymap.end(),
                   (std::string("Relation ")+relNames[0]+" does not exist").c_str());
          return mymap[relNames[0]].numTuples;
        }
	while (andlist != NULL){
		if (andlist->left != NULL){
			orlist = andlist->left;
			double fractionOr = 0.0;
			map<string, int>::iterator lastAtt;
			while(orlist != NULL){
				if (orlist->left->left->code == 3 && orlist->left->right->code == 3){
					map<string, int>::iterator itAtt[2];
					map<string, relInfo>::iterator itRel[2];
					string joinAtt1(orlist->left->left->value);
					string joinAtt2(orlist->left->right->value);

					for (map<string, relInfo>::iterator iit = mymap.begin(); iit!=mymap.end(); ++iit){
						itAtt[0] = iit->second.attrs.find(joinAtt1);
						if(itAtt[0] != iit->second.attrs.end()){
							itRel[0] = iit;
							break;
						}
					}
					for (map<string, relInfo>::iterator iit = mymap.begin(); iit!=mymap.end(); ++iit){
						itAtt[1] = iit->second.attrs.find(joinAtt2);
						if(itAtt[1] != iit->second.attrs.end()){
							itRel[1] = iit;
							break;
						}
					}
					
					double max;
					if (itAtt[0]->second >= itAtt[1]->second)		max = (double)itAtt[0]->second;
					else		max = (double)itAtt[1]->second;
					if (state == 0)
						result = (double)itRel[0]->second.numTuples*(double)itRel[1]->second.numTuples/max;
					else
						result = result*(double)itRel[1]->second.numTuples/max;
					

					state = 1;
				}
				else{
					string seleAtt(orlist->left->left->value);
					map<string, int>::iterator itAtt;
					map<string, relInfo>::iterator itRel;
					for (map<string, relInfo>::iterator iit = mymap.begin(); iit!=mymap.end(); ++iit){
						itAtt = iit->second.attrs.find(seleAtt);
						if(itAtt != iit->second.attrs.end()){
							itRel = iit;
							break;
						}
					}
					if (result == 0.0)
						result = ((double)itRel->second.numTuples);
					double tempFrac;
					if(orlist->left->code == 7)
						tempFrac = 1.0 / itAtt->second;
					else
						tempFrac = 1.0 / 3.0;
					if(lastAtt != itAtt)
						fractionOr = tempFrac+fractionOr-(tempFrac*fractionOr);
					else
						fractionOr += tempFrac;
					//cout << "fracOr: " << fractionOr << endl;
					lastAtt = itAtt;//
				}
				orlist = orlist->rightOr;
			}
			if (fractionOr != 0.0)
				fraction = fraction*fractionOr;
			//cout << "frac: " << fraction << endl;
		}
		andlist = andlist->rightAnd;
	}
	result = result * fraction;
	//cout << "result " << result << endl;
	tempRes = result;
	return result;
}


Statistics::~Statistics(){}
