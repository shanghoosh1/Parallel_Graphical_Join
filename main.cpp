/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/* 
 * File:   newmain.cpp
 * Author: 
 *
 * Created on 4 February 2022, 09:16
 */

#include <cstdlib>
#include <chrono>
#include <omp.h>
#include <iostream>
#include <unordered_map>
 #include <random>
#include <string>
#include <stdio.h>
#include <stdlib.h>
#include "csv_reader.hpp" 

using namespace std;

void mapAdd(unordered_map<unsigned int, unsigned long long int> &inout, unordered_map<unsigned int, unsigned long long int>  &in) {
   
        for (auto &entry:in)
//            if(inout.find(entry.first)!=inout.end())
                inout[entry.first]+=entry.second;
   
}

void conMapAdd(unordered_map<unsigned int, unordered_map<unsigned int,unsigned long long int>> &inout, unordered_map<unsigned int, unordered_map<unsigned int,unsigned long long int>> &in) {
   
        for (auto &entry:in)
            for(auto &entry2:entry.second)
                inout[entry.first][entry2.first]+=entry2.second;
   
}
void GFJS_reduc(unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > > &inout, unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  &in){
    for (auto &entry:in)
            inout[entry.first]=entry.second;
}

#pragma omp declare reduction(mapAdd : \
        unordered_map<unsigned int, unsigned long long int> : \
        mapAdd(omp_out, omp_in) \
      ) initializer (omp_priv=omp_orig)
#pragma omp declare reduction(conAdd : \
    unordered_map<unsigned int, unordered_map<unsigned int,unsigned long long int>> :    \
    conMapAdd(omp_out, omp_in)  \
  ) initializer (omp_priv=omp_orig)

#pragma omp declare reduction(gfjs_reduc : \
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  :    \
    GFJS_reduc(omp_out, omp_in)  \
  ) initializer (omp_priv=omp_orig)


void generateSynthData(long long int size, int num1,int num2, string outAdd){
    /* Seed */
    std::random_device rd;

    /* Random number generator */
    std::default_random_engine generator(rd());

    /* Distribution on which to apply the generator */
    std::uniform_int_distribution<long long unsigned> distribution1(0,num1);
    std::uniform_int_distribution<long long unsigned> distribution2(0,num2);
    vector<vector<unsigned long long int>> rawData; 
    for(long long int i=0; i<size;i++){
        rawData.push_back({distribution1(generator), distribution2(generator)});

    }

    std::ofstream out(outAdd);
    out<<"att1|att2\n";
    for (auto& row : rawData) {
      for (auto col : row)
        out << col <<'|';
      out << '\n';
    }
}



void sequentialSum_produc(vector<vector<string>> &rawData, int sumoutAtt, unordered_map<unsigned int, unsigned long long int> &singleMap,unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  &GFJS,vector<unordered_map<unsigned int, unsigned long long int>> & childMap){
    int firstVar= sumoutAtt; int secondVar=1-sumoutAtt; 
    unordered_map<unsigned int, unordered_map<unsigned int,unsigned long long int>> conMap;
    
    for (auto &row : rawData){
        conMap[stoi(row[firstVar])][stoi(row[secondVar])] += 1;
    }

    if (childMap.size()==0){
        for(int i = 0; i < conMap.size(); i++) {
            auto datIt = conMap.begin();
            advance(datIt, i);
            unsigned long long int count=0;
            for(auto &ent:(*datIt).second){
                    GFJS[(*datIt).first].push_back({ent.first,{ent.second,1}})  ; 
                    count+=ent.second;
            }
            if(count!=0)
            {
                 singleMap[(*datIt).first]=count;
            }
        }
    }
    else if (childMap.size()==1){
        auto &child=childMap[0];
        for(int i = 0; i < conMap.size(); i++) {
            auto datIt = conMap.begin();
            advance(datIt, i);
            unsigned long long int count=0;
            for(auto &ent:(*datIt).second){
                if(child.find(ent.first)!=child.end()) {
                    GFJS[(*datIt).first].push_back({ent.first,{ent.second,child[ent.first]}})  ; 
                    count+=ent.second*child[ent.first];
                }
            }
            if(count!=0)
            {
                 singleMap[(*datIt).first]=count;
            }
        }
    }
    else if (childMap.size()==2){
        auto &child1=childMap[0];
        auto &child2=childMap[1];
        for(int i = 0; i < conMap.size(); i++) {
            auto datIt = conMap.begin();
            advance(datIt, i);
            unsigned long long int count=0;
            for(auto &ent:(*datIt).second){
                if(child1.find(ent.first)!=child1.end()&& child2.find(ent.first)!=child2.end()) {
                    GFJS[(*datIt).first].push_back({ent.first,{ent.second,child1[ent.first]*child2[ent.first]}})  ; 
                    count+=ent.second*child1[ent.first]*child2[ent.first];
                }
            }
            if(count!=0)
            {
                 singleMap[(*datIt).first]=count;
            }
        }
    }
    else if (childMap.size()==3){
        auto &child1=childMap[0];
        auto &child2=childMap[1];
        auto &child3=childMap[2];
        for(int i = 0; i < conMap.size(); i++) {
            auto datIt = conMap.begin();
            advance(datIt, i);
            unsigned long long int count=0;
            for(auto &ent:(*datIt).second){
                if(child1.find(ent.first)!=child1.end()&& child2.find(ent.first)!=child2.end() && child3.find(ent.first)!=child3.end()) {
                    GFJS[(*datIt).first].push_back({ent.first,{ent.second,child1[ent.first]*child2[ent.first]*child3[ent.first]}})  ; 
                    count+=ent.second*child1[ent.first]*child2[ent.first]*child3[ent.first];
                }
            }
            if(count!=0)
            {
                 singleMap[(*datIt).first]=count;
            }
        }
    }
}

void parallelSum_produc(vector<vector<string>> &rawData, int sumoutAtt, unordered_map<unsigned int, unsigned long long int> &singleMap,unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  &GFJS, vector<unordered_map<unsigned int, unsigned long long int>> & childMap){
    int firstVar= sumoutAtt; int secondVar=1-sumoutAtt;     
    
    unordered_map<unsigned int, unordered_map<unsigned int,unsigned long long int>> conMap;
    
    #pragma omp parallel for  reduction(conAdd : conMap)
    for (unsigned long long int i = 0; i < rawData.size(); i++) {
         conMap[stoi(rawData[i][firstVar])][stoi(rawData[i][secondVar])] += 1;
     }

    if(childMap.size()==0){
        #pragma omp parallel for reduction(mapAdd : singleMap) reduction(gfjs_reduc:GFJS)
        for(int i = 0; i < conMap.size(); i++) {
            auto datIt = conMap.begin();
            advance(datIt, i);
            unsigned long long int count=0;
            for(auto &ent:(*datIt).second){
                    GFJS[(*datIt).first].push_back({ent.first,{ent.second,1}})  ; 
                    count+=ent.second;
            }
           if(count!=0)
            {
                 singleMap[(*datIt).first]=count;
            }
        }
    }
    else if(childMap.size()==1){
        #pragma omp parallel for reduction(mapAdd : singleMap) reduction(gfjs_reduc:GFJS)
        for(int i = 0; i < conMap.size(); i++) {
            auto datIt = conMap.begin();
            advance(datIt, i);
            unsigned long long int count=0;
            auto &child1=childMap[0];
            for(auto &ent:(*datIt).second){
                if(child1.find(ent.first)!=child1.end()) {
                    GFJS[(*datIt).first].push_back({ent.first,{ent.second,child1[ent.first]}})  ; 
                    count+=ent.second*child1[ent.first];
                }
            }
           if(count!=0)
            {
                 singleMap[(*datIt).first]=count;
            }
        }
    }
    else if(childMap.size()==2){
        #pragma omp parallel for reduction(mapAdd : singleMap) reduction(gfjs_reduc:GFJS)
        for(int i = 0; i < conMap.size(); i++) {
            auto datIt = conMap.begin();
            advance(datIt, i);
            unsigned long long int count=0;
            auto &child1=childMap[0];
            auto &child2=childMap[1];
            for(auto &ent:(*datIt).second){
                if(child1.find(ent.first)!=child1.end() && child2.find(ent.first)!=child2.end()) {
                    GFJS[(*datIt).first].push_back({ent.first,{ent.second,child1[ent.first]*child2[ent.first]}})  ; 
                    count+=ent.second*child1[ent.first]*child2[ent.first];
                }
            }
           if(count!=0)
            {
                 singleMap[(*datIt).first]=count;
            }
        }
    }
    else if(childMap.size()==3){
        #pragma omp parallel for reduction(mapAdd : singleMap) reduction(gfjs_reduc:GFJS)
        for(int i = 0; i < conMap.size(); i++) {
            auto datIt = conMap.begin();
            advance(datIt, i);
            unsigned long long int count=0;
            auto &child1=childMap[0];
            auto &child2=childMap[1];
            auto &child3=childMap[2];
            for(auto &ent:(*datIt).second){
                if(child1.find(ent.first)!=child1.end() && child2.find(ent.first)!=child2.end()&& child3.find(ent.first)!=child3.end()) {
                    GFJS[(*datIt).first].push_back({ent.first,{ent.second,child1[ent.first]*child2[ent.first]*child3[ent.first]}})  ; 
                    count+=ent.second*child1[ent.first]*child2[ent.first]*child3[ent.first];
                }
            }
           if(count!=0)
            {
                 singleMap[(*datIt).first]=count;
            }
        }
    }

    
}



void do_seq_lastFM_A1(int generationMode, vector<vector<string>> &ua, vector<vector<string>> &uf,vector<vector<string>> &ua1){
    
    //      the graph:      w --> u <--u1 <-- w1
    
    auto start = std::chrono::system_clock::now();
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed; 
    
    vector<unordered_map<unsigned int, unsigned long long int>> childMapL1; // empty
    
    vector<unordered_map<unsigned int, unsigned long long int>> w2u; // childs for the upper level
    unordered_map<unsigned int, unsigned long long int> cm1;
    w2u.push_back(cm1);
    
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u2wGrn;
    sequentialSum_produc(ua, 0,w2u[0], u2wGrn,childMapL1); //w is being elimianted
    
    vector<unordered_map<unsigned int, unsigned long long int>> w1_2_u1;
    unordered_map<unsigned int, unsigned long long int> cm2;
    w1_2_u1.push_back(cm2);
    
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u1_to_w1Gen;
    sequentialSum_produc(ua1, 0,w1_2_u1[0], u1_to_w1Gen,childMapL1); // w1 is being eliminated
    
    unordered_map<unsigned int, unsigned long long int> cm3;
    w2u.push_back(cm3);
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u_to_u1Gen;
    sequentialSum_produc(uf, 0,w2u[1], u_to_u1Gen,w1_2_u1);  // u1 is being eliminated

    ///  NB: there are two potentials in the root
    
    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n sequential inference in: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
    long long int coun=0;
    for(auto &x:w2u[0])
        if(w2u[1].find(x.first)!=w2u[1].end())
            coun+=x.second * w2u[1][x.first];
    cout<<"Join size is: "<<coun<<endl;
    
      //start result generation in memory and throw it away 
    
    if(generationMode==0){ // columnar generation
        long int max_vec_initialization=200000000;
        vector<vector<unsigned long long int>> levelData(4);
        for(auto &root:w2u[0]){
            if(w2u[1].find(root.first)!=w2u[1].end()){
                auto rootVal=root.second*w2u[1][root.first];    
                if(rootVal<max_vec_initialization){
                    levelData[0].assign(rootVal,root.first ); 
                }
                else{
                      for(int i=0;i<rootVal/ max_vec_initialization;i++)
                          levelData[0].assign(max_vec_initialization,root.first );
                      levelData[0].assign(rootVal% max_vec_initialization,root.first );
                }

                for(auto &w:u2wGrn[root.first]){
                    for(auto &u1:u_to_u1Gen[root.first]){
                        unsigned long long int bucket=w.second[0]* u1.second[0];
                        unsigned long long int value=bucket* w.second[1]*u1.second[1];
                        if(value<max_vec_initialization){
                            levelData[1].assign(value,w.first); 
                            levelData[2].assign(value,u1.first); 
                        }
                        else{
                              for(int i=0;i<value / max_vec_initialization;i++){
                                  levelData[1].assign(max_vec_initialization,w.first);
                                  levelData[2].assign(max_vec_initialization,u1.first);
                              }
                              levelData[1].assign(value% max_vec_initialization,w.first);
                              levelData[2].assign(value% max_vec_initialization,u1.first);
                        }

                        for (auto &w1:u1_to_w1Gen[u1.first]){
                            unsigned long long int value=bucket* w1.second[0]*w1.second[1];
                            if(value<max_vec_initialization){
                                levelData[3].assign(value,w1.first); 
                            }
                            else{
                                  for(int i=0;i<value / max_vec_initialization;i++){
                                      levelData[3].assign(max_vec_initialization,w1.first);
                                  }
                                  levelData[3].assign(value% max_vec_initialization,w1.first);
                            }

                            //If you wanna print all the tuples
    //                        cout<<w.first<<",  "<<root.first<<",  "<<u1.first<<",  "<<w1.first<<", freq:  "<<value<<endl;
                        }

                    }
                }
            }
        }
        
    }
    else if(generationMode==1){
        //to test FDB row oriented generation
        
        long int count=0;
        vector<unsigned long long int> levelData(4);
        for(auto &root:w2u[0]){
            if(w2u[1].find(root.first)!=w2u[1].end()){
                for(auto &w:u2wGrn[root.first]){
                    for(auto &u1:u_to_u1Gen[root.first]){
                        unsigned long long int bucket=w.second[0]* u1.second[0];
                        
                        for (auto &w1:u1_to_w1Gen[u1.first]){
                            unsigned long long int value=bucket* w1.second[0]*w1.second[1];
                            count+=value;
                            for(int rows=0;rows<value;rows++)
                            {
                                levelData={root.first, u1.first, w.first, w1.first};
                            }
                            //If you wanna print all the tuples
    //                        cout<<w.first<<",  "<<root.first<<",  "<<u1.first<<",  "<<w1.first<<", freq:  "<<value<<endl;
                        }


                    }
                }
            }
        }
        cout<<"the join size with FDB: "<<count<<endl;
        
    }
    

    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n sequential in memory generation in: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
//    
//    
//    
//    //Summary generation and inserting to the disk
//    
//    
// vector<vector<unsigned long long int>> levelFreqs(4);
//    for(auto &root:w2u[0]){
//        if(w2u[1].find(root.first)!=w2u[1].end()){   
//            levelFreqs[0].push_back(root.first ); 
//            levelFreqs[0].push_back(root.second*w2u[1][root.first] ); 
//   
//            for(auto &w:u2wGrn[root.first]){
//                for(auto &u1:u_to_u1Gen[root.first]){
//                    unsigned long long int bucket=w.second[0]* u1.second[0];
//                    unsigned long long int value=bucket* w.second[1]*u1.second[1];
//                    levelFreqs[1].push_back(w.first); 
//                    levelFreqs[1].push_back(value); 
//                    levelFreqs[2].push_back(u1.first); 
//                    levelFreqs[2].push_back(value); 
//                    
//                    for (auto &w1:u1_to_w1Gen[u1.first]){
//                        levelFreqs[3].push_back(w1.first);
//                        levelFreqs[3].push_back(bucket* w1.second[0]*w1.second[1]);
//  
//                        //If you wanna print all the tuples
////                        cout<<w.first<<",  "<<root.first<<",  "<<u1.first<<",  "<<w1.first<<", freq:  "<<value<<endl;
//                    }
//                    
//                    //***********************************  
//                    //********************************
//                    // Why GFJS generation is slower than full join result generation in memory?????????????????????????????????
//                   
//                }
//            }
//        }
//    }
//    
//    end = std::chrono::system_clock::now();
//    elapsed = end - start;
//    std::cout << "\n sequential GFJS generation in memory: " << elapsed.count() << "s"<<endl;    
//    start = std::chrono::system_clock::now();
//    
//    
//    // inserting the summary to the disk seq
//
//    for(int f=0;f<levelFreqs.size();f++){
//        string fileName="/media/ali/2TB/output/"+to_string(f)+".txt";
//        ofstream output_file(fileName);
//        ostream_iterator<long long int> output_iterator(output_file, "|");
//        copy(levelFreqs[f].begin(), levelFreqs[f].end(), output_iterator);
//        output_file.close();
//    }
//       
//    end = std::chrono::system_clock::now();
//    elapsed = end - start;
//    std::cout << "\n sequential storing  in: " << elapsed.count() << "s"<<endl;    
//    start = std::chrono::system_clock::now();
//    
//
//    
//    //load and de-summarize seq
//      max_vec_initialization=200000000;
//    for(int f=0;f<levelFreqs.size();f++){
//        string fileName="/media/ali/2TB/output/"+to_string(f)+".txt";
//        std::ifstream file(fileName);
//        if (!file.good())
//            continue;
//        string line = "";
//        getline(file, line);
//        string::const_iterator start = line.begin();
//        string::const_iterator end = line.end();
//        string::const_iterator next = std::find(start, end, '|');
//        int key;
//        unsigned long long int value;
//        while (next != end)
//        {
//            key = stoi(string(start, next));
//            start = next + 1;
//            next = find(start, end, '|');
//            value=stoll(string(start, next));
//            start = next + 1;
//            next = find(start, end, '|');
//
//
//            if(value<max_vec_initialization){
//                vector <int> tmp;
//                tmp.assign(value, key);
//            }
//            else{
//                for(int i=0;i<value / max_vec_initialization;i++){
//                    vector <int> tmp;
//                    tmp.assign(max_vec_initialization, key);
//
//                }
//                vector <int> tmp;
//                tmp.assign(value % max_vec_initialization, key); // remaining
//            }
//
//        }
////        cout<< "Att "<<f<< " loaded."<<endl;
//    }
//    
//    end = std::chrono::system_clock::now();
//    elapsed = end - start;
//    std::cout << "\n sequential desummarization in: " << elapsed.count() << "s"<<endl;    
//    start = std::chrono::system_clock::now();
}


void do_parallel_lastFM_A2(vector<vector<string>> &ua1, vector<vector<string>> &uf1, vector<vector<string>> &uf2,vector<vector<string>> &ua3, int numThreads){
   // the graph:      a1 --> u1 (--> u2 <--) u3 <-- a3
    
    auto start = std::chrono::system_clock::now();
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed; 

    vector<unordered_map<unsigned int, unsigned long long int>> childMapL1; // empty
    
    unordered_map<unsigned int, unsigned long long int> u1; // childs for the upper level
    unordered_map<unsigned int, unsigned long long int> u11;
    
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u1_a1_gen;
    
    vector<unordered_map<unsigned int, unsigned long long int>> a3_u3; // childs for the upper level
    unordered_map<unsigned int, unsigned long long int> cm1;
    a3_u3.push_back(cm1);
    
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u3_a3_gen;

  
 //delete u2


    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u1_u3_gen;

    unordered_map<unsigned int, unordered_map<unsigned int,unsigned long long int>> conMap1;
    unordered_map<unsigned int, unordered_map<unsigned int,unsigned long long int>> conMap2;
    unordered_map<unsigned int, unordered_map<unsigned int,unsigned long long int>> conMap3;
    
    omp_set_nested(2);
    omp_set_num_threads(numThreads);
    
    
    #pragma omp parallel
    {
        if( omp_get_num_threads()==0)
             printf("Number of threads = %d\n", omp_get_num_threads());
    #pragma omp single 
        {
            #pragma omp task
                parallelSum_produc(ua1, 0,u1, u1_a1_gen,childMapL1);
            #pragma omp task 
                parallelSum_produc(ua3, 0,a3_u3[0], u3_a3_gen,childMapL1);
            #pragma omp taskwait
        }

    }

    omp_set_nested(2);
    #pragma omp parallel
    {
        if( omp_get_num_threads()==0)
             cout<<"Number of threads = "<< omp_get_num_threads()<<endl;
    #pragma omp single 
        {
            #pragma omp task
            {
                #pragma omp parallel for  reduction(conAdd : conMap1)
                for (auto &row : uf1){
                    conMap1[stoi(row[1])][stoi(row[0])] += 1;
                }
            }
            #pragma omp task 
            {
                #pragma omp parallel for  reduction(conAdd : conMap2)
                for (auto &row : uf2){
                    conMap2[stoi(row[0])][stoi(row[1])] += 1;
                }
            }
            #pragma omp taskwait
        }

    }
    
    
    
    #pragma omp parallel for  reduction(conAdd : conMap3)
    for(int i = 0; i < conMap1.size(); i++) {
        auto x = conMap1.begin();
        advance(x, i);
        for(auto &y: (*x).second){
            if(conMap2.find(y.first)!=conMap2.end()){
                for(auto &z: conMap2[y.first]){
                    conMap3[(*x).first][z.first]+= y.second*z.second;
                }
                
            }
        }
    }
    
    auto &child=a3_u3[0];

    #pragma omp parallel for reduction(mapAdd : u11) reduction(gfjs_reduc:u1_u3_gen)
    for(int i = 0; i < conMap3.size(); i++) {
        auto datIt = conMap3.begin();
        advance(datIt, i);
        unsigned long long int count=0;
        for(auto &ent:(*datIt).second){
            if(child.find(ent.first)!=child.end()) {
                u1_u3_gen[(*datIt).first].push_back({ent.first,{ent.second,child[ent.first]}})  ; 
                count+=ent.second*child[ent.first];
            }
        }
        if(count!=0)
        {
             u11[(*datIt).first]=count;
        }
    }

    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n parallel inference in: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
//    long long int coun=0;
//    for(auto &x:u1)
//        if(u11.find(x.first)!=u11.end())
//            coun+=x.second * u11[x.first];
//    cout<<"Join size is: "<<coun<<endl;
    
    //    //Result generation in the mem and throw it away
    long int max_vec_initialization=200000000;
    vector<vector<vector<unsigned long long int>>> levelData(numThreads,vector<vector<unsigned long long int>>(4));
//    
   #pragma omp parallel for  
    for(int i = 0; i < u1.size(); i++) {
        auto root = u1.begin();
        advance(root, i);
        int th_num=omp_get_thread_num();
        if(u11.find((*root).first)!= u11.end()){
            auto val=(*root).second * u11[(*root).first];
            if(val<max_vec_initialization){
                levelData[th_num][0].assign(val,(*root).first ); 
            }
            else{
                  for(int i=0;i<val/ max_vec_initialization;i++)
                      levelData[th_num][0].assign(max_vec_initialization,(*root).first );
                  levelData[th_num][0].assign(val% max_vec_initialization,(*root).first );
            }
            
            for (auto &a1:u1_a1_gen[(*root).first]){
                for (auto &u3:u1_u3_gen[(*root).first]){
                    auto bucket=a1.second[0]* u3.second[0];
                    auto val= bucket * a1.second[1] * u3.second[1];
                    if(val<max_vec_initialization){
                        levelData[th_num][1].assign(val,a1.first); 
                        levelData[th_num][2].assign(val,u3.first);
                    }
                    else{
                          for(int i=0;i<val / max_vec_initialization;i++){
                              levelData[th_num][1].assign(max_vec_initialization,a1.first);
                              levelData[th_num][2].assign(max_vec_initialization,u3.first);
                          }
                          levelData[th_num][1].assign(val% max_vec_initialization,a1.first);
                          levelData[th_num][2].assign(val% max_vec_initialization,u3.first);
                    }
                    
                    for (auto &a3:u3_a3_gen[u3.first]){
                        auto value=bucket*a3.second[0]*a3.second[1];
                        if(value<max_vec_initialization){
                            levelData[th_num][3].assign(value,a3.first); 
                        }
                        else{
                              for(int i=0;i<value / max_vec_initialization;i++){
                                  levelData[th_num][3].assign(max_vec_initialization,a3.first);
                              }
                              levelData[th_num][3].assign(value% max_vec_initialization,a3.first);
                        }
                    }
                }
            }
        }
    }

    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n parallel generation in memory: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
    
    ////////////////////////
    
       //Summary generation and inserting to the disk
    
    
    vector<vector<vector<unsigned long long int>>> levelFreqs(numThreads,vector<vector<unsigned long long int>>(4));
    omp_set_num_threads(numThreads);
    #pragma omp parallel for 
    for(int i = 0; i < u1.size(); i++) {
        auto root = u1.begin();
        advance(root, i);
        int th_num=omp_get_thread_num();
        if(u11.find((*root).first)!= u11.end()){
            auto val=(*root).second * u11[(*root).first];
            levelFreqs[th_num][0].push_back((*root).first ); 
            levelFreqs[th_num][0].push_back(val); 
            for (auto &a1:u1_a1_gen[(*root).first]){
                for (auto &u3:u1_u3_gen[(*root).first]){
                    auto bucket=a1.second[0]* u3.second[0];
                    auto val= bucket * a1.second[1] * u3.second[1];
                    levelFreqs[th_num][1].push_back(a1.first); 
                    levelFreqs[th_num][1].push_back(val);
                    levelFreqs[th_num][2].push_back(u3.first); 
                    levelFreqs[th_num][2].push_back(val);
                    for (auto &a3:u3_a3_gen[u3.first]){
                        auto value=bucket*a3.second[0]*a3.second[1];
                        levelFreqs[th_num][3].push_back(a3.first); 
                        levelFreqs[th_num][3].push_back(value);
                    }
                }
            }
        }
    }

    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n parallel gfjs generation in memory: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
    // inserting the summary to the disk
    #pragma omp parallel
    {
        int th_num= omp_get_thread_num();
//        cout<<th_num<<endl;
        for(int f=0;f<levelFreqs[th_num].size();f++){
            string fileName="/media/ali/2TB/output/"+to_string(th_num)+"_"+to_string(f)+".csv";
            ofstream output_file(fileName);
            ostream_iterator<long long int> output_iterator(output_file, "|");
            copy(levelFreqs[th_num][f].begin(), levelFreqs[th_num][f].end(), output_iterator);
            output_file.close();
        }
    }
       
    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n parallel storing: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
    //load and de-summarize in parallel
     //load and de-summarize seq
    max_vec_initialization=200000000;
    #pragma omp parallel
    {
        int th_num= omp_get_thread_num();
        for(int f=0;f<levelFreqs[th_num].size();f++){
            string fileName="/media/ali/2TB/output/"+to_string(th_num)+"_"+to_string(f)+".csv";
            std::ifstream file(fileName);
            if (!file.good())
            {
                cout<<"file not exists"<<endl;
                exit(1);
            }
            string line = "";
            getline(file, line);
            string::const_iterator start = line.begin();
            string::const_iterator end = line.end();
            string::const_iterator next = std::find(start, end, '|');
            int key;
            unsigned long long int value;
            while (next != end)
            {
                key = stoi(string(start, next));
                start = next + 1;
                next = find(start, end, '|');
                value=stoll(string(start, next));
                start = next + 1;
                next = find(start, end, '|');


                if(value<max_vec_initialization){
                    vector <int> tmp;
                    tmp.assign(value, key);
                }
                else{
                    for(int i=0;i<value / max_vec_initialization;i++){
                        vector <int> tmp;
                        tmp.assign(max_vec_initialization, key);

                    }
                    vector <int> tmp;
                    tmp.assign(value % max_vec_initialization, key); // remaining
                }

            }
//            cout<< fileName<<endl;
        }
    }
    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n parallel loading in: " << elapsed.count() << "s"<<endl;    
//   
}

void do_parallel_lastFM_A2_select_star(vector<vector<string>> &ua1, vector<vector<string>> &uf1, vector<vector<string>> &uf2,vector<vector<string>> &ua3, int numThreads){
   // the graph:      a1 --> u1 --> u2 <-- u3 <-- a3
    
    auto start = std::chrono::system_clock::now();
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed; 

    vector<unordered_map<unsigned int, unsigned long long int>> childMapL1; // empty

    
    vector<unordered_map<unsigned int, unsigned long long int>> a3_u3; 
    unordered_map<unsigned int, unsigned long long int> cm1;
    a3_u3.push_back(cm1);
    vector<unordered_map<unsigned int, unsigned long long int>> a1_u1; 
    a1_u1.push_back(cm1);
    vector<unordered_map<unsigned int, unsigned long long int>> u1_u2; 
    u1_u2.push_back(cm1);
    vector<unordered_map<unsigned int, unsigned long long int>> u3_u2;
    u3_u2.push_back(cm1);
    
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u1_a1_gen;
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u3_a3_gen;
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u2_u1_gen;
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u2_u3_gen;

    
    omp_set_nested(2);
    omp_set_num_threads(numThreads);
    
    
    #pragma omp parallel
    {
        if( omp_get_num_threads()==0)
             printf("Number of threads = %d\n", omp_get_num_threads());
    #pragma omp single 
        {
            #pragma omp task
                parallelSum_produc(ua1, 0,a1_u1[0], u1_a1_gen,childMapL1);
            #pragma omp task 
                parallelSum_produc(ua3, 0,a3_u3[0], u3_a3_gen,childMapL1);
            #pragma omp taskwait
            
            #pragma omp task
                parallelSum_produc(uf1, 0,u1_u2[0], u1_a1_gen,a1_u1);
            #pragma omp task 
                parallelSum_produc(uf2, 0,u3_u2[0], u3_a3_gen,a3_u3);
            #pragma omp taskwait
        }
    }
    
    

    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n parallel inference in: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
    long long int coun=0;
    for(auto &x:u1_u2[0])
        if(u3_u2[0].find(x.first)!=u3_u2[0].end())
            coun+=x.second * u3_u2[0][x.first];
    cout<<"Join size is: "<<coun<<endl;
    
    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n count calculation time: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
 
//   
}
void do_seq_lastFM_A2_select_star(int generationMode, vector<vector<string>> &ua1, vector<vector<string>> &uf1, vector<vector<string>> &uf2,vector<vector<string>> &ua3){
    
auto start = std::chrono::system_clock::now();
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed; 

    vector<unordered_map<unsigned int, unsigned long long int>> childMapL1; // empty

    
    vector<unordered_map<unsigned int, unsigned long long int>> a3_u3; 
    unordered_map<unsigned int, unsigned long long int> cm1;
    a3_u3.push_back(cm1);
    vector<unordered_map<unsigned int, unsigned long long int>> a1_u1; 
    a1_u1.push_back(cm1);
    vector<unordered_map<unsigned int, unsigned long long int>> u1_u2; 
    u1_u2.push_back(cm1);
    vector<unordered_map<unsigned int, unsigned long long int>> u3_u2;
    u3_u2.push_back(cm1);
    
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u1_a1_gen;
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u3_a3_gen;
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u2_u1_gen;
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u2_u3_gen;

    sequentialSum_produc(ua1, 0,a1_u1[0], u1_a1_gen,childMapL1);
    sequentialSum_produc(ua3, 0,a3_u3[0], u3_a3_gen,childMapL1);
    sequentialSum_produc(uf1, 0,u1_u2[0], u2_u1_gen,a1_u1);
    sequentialSum_produc(uf2, 0,u3_u2[0], u2_u3_gen,a3_u3);

 

    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n seq inference in: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
    long long int coun=0;
    for(auto &x:u1_u2[0])
        if(u3_u2[0].find(x.first)!=u3_u2[0].end())
            coun+=x.second * u3_u2[0][x.first];
    cout<<"Join size is: "<<coun<<endl;
    long int max_vec_initialization=200000000;
    if(generationMode==0){
        vector<vector<unsigned long long int>> levelData(5);
        for(auto &root:u1_u2[0])
            if(u3_u2[0].find(root.first)!=u3_u2[0].end()){
                auto rootVal=root.second*u3_u2[0][root.first];    
               
                if(rootVal<max_vec_initialization){
                     levelData[0].assign(rootVal,root.first );
                }
                else{
                      for(int i=0;i<rootVal/ max_vec_initialization;i++)
                          levelData[0].assign(max_vec_initialization,root.first );
                      levelData[0].assign(rootVal% max_vec_initialization,root.first );
                }
                for(auto &u1:u2_u1_gen[root.first]){
                    for(auto &u3: u2_u3_gen[root.first]){
                        auto bucket=u1.second[0] * u3.second[0];
                        auto val= bucket * u1.second[1] * u3.second[1];
                        if(val<max_vec_initialization){
                            levelData[1].assign(val,u1.first );
                            levelData[2].assign(val,u3.first );
                       }
                       else{
                             for(int i=0;i<val/ max_vec_initialization;i++){
                                 levelData[1].assign(max_vec_initialization,u1.first );
                                 levelData[2].assign(max_vec_initialization,u3.first );
                             }
                             levelData[1].assign(val%max_vec_initialization,u1.first );
                             levelData[2].assign(val%max_vec_initialization,u3.first );
                       }

                        for(auto &a1:u1_a1_gen[u1.first]){
                            for(auto &a3:u3_a3_gen[u3.first]){
                                auto val=bucket*a1.second[0]*a3.second[0]*a1.second[1]*a3.second[1];
                                
                               if(val<max_vec_initialization){
                                    levelData[3].assign(val,a1.first);
                                    levelData[4].assign(val,a3.first);
                               }
                               else{
                                     for(int i=0;i<val/ max_vec_initialization;i++){
                                         levelData[3].assign(max_vec_initialization,a1.first );
                                         levelData[4].assign(max_vec_initialization,a3.first );
                                     }
                                     levelData[3].assign(val%max_vec_initialization,a1.first );
                                     levelData[4].assign(val%max_vec_initialization,a3.first );
                               } 
                            }
                        }
                    }
                }
            }
    }
    else if(generationMode==1){
        vector<unsigned long long int> levelData(5);
        for(auto &root:u1_u2[0])
            if(u3_u2[0].find(root.first)!=u3_u2[0].end()){
                for(auto &u1:u2_u1_gen[root.first]){
                    for(auto &u3: u2_u3_gen[root.first]){
                        auto bucket=u1.second[0] * u3.second[0];
                        for(auto &a1:u1_a1_gen[u1.first]){
                            for(auto &a3:u3_a3_gen[u3.first]){
                                auto val=bucket*a1.second[0]*a3.second[0]*a1.second[1]*a3.second[1];
                                for(int rows=0;rows<val;rows++)
                                    levelData={root.first, u1.first,u3.first,a3.first,a1.first};
                                
                            }
                        }
                    }
                }
            }
    }

    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n count calculation time: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
}
void do_seq_lastFM_A2(int generationMode, vector<vector<string>> &ua1, vector<vector<string>> &uf1, vector<vector<string>> &uf2,vector<vector<string>> &ua3){
    

    // the graph:      a1 --> u1 (--> u2 <--) u3 <-- a3
    
    auto start = std::chrono::system_clock::now();
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed; 
    
    
    
    vector<unordered_map<unsigned int, unsigned long long int>> childMapL1; // empty
    
    vector<unordered_map<unsigned int, unsigned long long int>> u1; // childs for the upper level
    unordered_map<unsigned int, unsigned long long int> cm1;
    u1.push_back(cm1);
    
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u1_a1_gen;
    sequentialSum_produc(ua1, 0,u1[0], u1_a1_gen,childMapL1); 
    
    vector<unordered_map<unsigned int, unsigned long long int>> a3_u3; // childs for the upper level
    a3_u3.push_back(cm1);
    
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u3_a3_gen;
    sequentialSum_produc(ua3, 0,a3_u3[0], u3_a3_gen,childMapL1); 
    
//    vector<unordered_map<unsigned int, unsigned long long int>> u2; // the root
//    u2.push_back(cm1);
//    u2.push_back(cm1);
//    
//    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u2_u3_gen;
//    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u2_u1_gen;
//    sequentialSum_produc(uf1, 1,u2[0], u2_u1_gen,a1_u1); 
//    sequentialSum_produc(uf2, 0,u2[1], u2_u3_gen,a3_u3); 
  
 //delete u2

    u1.push_back(cm1);
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u1_u3_gen;

    unordered_map<unsigned int, unordered_map<unsigned int,unsigned long long int>> conMap1;
    unordered_map<unsigned int, unordered_map<unsigned int,unsigned long long int>> conMap2;
    unordered_map<unsigned int, unordered_map<unsigned int,unsigned long long int>> conMap3;
    
    for (auto &row : uf1){
        conMap1[stoi(row[1])][stoi(row[0])] += 1;
    }
    for (auto &row : uf2){
        conMap2[stoi(row[0])][stoi(row[1])] += 1;
    }
    
    for(int i = 0; i < conMap1.size(); i++) {
        auto x = conMap1.begin();
        advance(x, i);
        for(auto &y: (*x).second){
            if(conMap2.find(y.first)!=conMap2.end()){
                for(auto &z: conMap2[y.first]){
                    conMap3[(*x).first][z.first]+= y.second*z.second;
                }
                
            }
        }
    }
    
    auto &child=a3_u3[0];
    
    for(int i = 0; i < conMap3.size(); i++) {
        auto datIt = conMap3.begin();
        advance(datIt, i);
        unsigned long long int count=0;
        for(auto &ent:(*datIt).second){
            if(child.find(ent.first)!=child.end()) {
                u1_u3_gen[(*datIt).first].push_back({ent.first,{ent.second,child[ent.first]}})  ; 
                count+=ent.second*child[ent.first];
            }
        }
        if(count!=0)
        {
             u1[1][(*datIt).first]=count;
        }
    }

    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n sequential inference in: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
    long long int coun=0;
    for(auto &x:u1[0])
        if(u1[1].find(x.first)!=u1[1].end())
            coun+=x.second * u1[1][x.first];
    cout<<"Join size is: "<<coun<<endl;
//    
//      //start result generation in memory and throw it away 
//
    start = std::chrono::system_clock::now();
    if (generationMode==0){
        long int max_vec_initialization=200000000;
        vector<vector<unsigned long long int>> levelData(4);
        for(auto &root:u1[0]){
            if(u1[1].find(root.first)!=u1[1].end()){
                auto rootVal=root.second*u1[1][root.first];
    //            cout<<"----"<<root.first<<": "<<root.second<<endl;
                if(rootVal<max_vec_initialization){
                    levelData[0].assign(rootVal,root.first ); 
                }
                else{
                      for(int i=0;i<rootVal/ max_vec_initialization;i++)
                          levelData[0].assign(max_vec_initialization,root.first );
                      levelData[0].assign(rootVal% max_vec_initialization,root.first );
                }

                for(auto &a1:u1_a1_gen[root.first]){    //u1 and u3 generation
                    for(auto &u3:u1_u3_gen[root.first]){
                        auto bucket=a1.second[0]* u3.second[0];
                        auto value=bucket* a1.second[1]*u3.second[1];
    //                    cout<<"-----------"<<a1.first<<", "<<u3.first<<": "<<value<<endl;
                        if(value<max_vec_initialization){
                            levelData[1].assign(value,a1.first); 
                            levelData[2].assign(value,u3.first); 
                        }
                        else{
                              for(int i=0;i<value / max_vec_initialization;i++){
                                  levelData[1].assign(max_vec_initialization,a1.first);
                                  levelData[2].assign(max_vec_initialization,u3.first);
                              }
                              levelData[1].assign(value% max_vec_initialization,a1.first);
                              levelData[2].assign(value% max_vec_initialization,u3.first);
                        } 
    //                    cout<<u3_a3_gen[u3.first].size()<<endl;
                        for(auto &a3:u3_a3_gen[u3.first]){
                            auto value= bucket* a3.second[0]* a3.second[1];
    //                        cout<<"-----------------------"<<a3.first<<": "<<value<<endl;
                            if(value<max_vec_initialization){
                                levelData[3].assign(value,a3.first);  
                            }
                            else{
                                  for(int i=0;i<value / max_vec_initialization;i++){
                                      levelData[3].assign(max_vec_initialization,a3.first);
                                  }
                                  levelData[3].assign(value% max_vec_initialization,a3.first);
                            }                           
                        }   
                    }
                }
            }            
        }
    }
    else if(generationMode==1){
        vector<unsigned long long int> levelData(4);
        for(auto &root:u1[0]){
            if(u1[1].find(root.first)!=u1[1].end()){
                for(auto &a1:u1_a1_gen[root.first]){    //u1 and u3 generation
                    for(auto &u3:u1_u3_gen[root.first]){
                        auto bucket=a1.second[0]* u3.second[0];
                        for(auto &a3:u3_a3_gen[u3.first]){
                            auto value= bucket* a3.second[0]* a3.second[1];
                            for(int rows=0;rows<value;rows++)
                                levelData={root.first, a1.first,a3.first,u3.first};
                        }   
                    }
                }
            }            
        }
    }
    
    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n sequential in memory generation in: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
//    
//    
////    //Summary generation and inserting to the disk
////    
////
//    vector<vector<unsigned long long int>> levelFreqs(4);
//    for(auto &root:u1[0]){
//        if(u1[1].find(root.first)!=u1[1].end()){
//            auto rootVal=root.second*u1[1][root.first];    
////            cout<<root.first<<":  "<<rootVal<<endl;
//
//            levelFreqs[0].push_back(root.first );
//            levelFreqs[0].push_back(rootVal );
//            
//            for(auto &a1:u1_a1_gen[root.first]){    //u1 and u3 generation
//                for(auto &u3:u1_u3_gen[root.first]){
//                    auto bucket=a1.second[0]* u3.second[0];
//                    auto value=bucket* a1.second[1]*u3.second[1];
//                        levelFreqs[1].push_back(a1.first); 
//                        levelFreqs[1].push_back(value); 
//                        levelFreqs[2].push_back(u3.first); 
//                        levelFreqs[2].push_back(value);
//
//                    
////                    cout<<"-----------"<<u1.first<<", "<<u3.first<<", "<<value<<endl;
//                    
//                    for(auto &a3:u3_a3_gen[u3.first]){   // a1 and a3 generation
//                        
//                        levelFreqs[3].push_back(a3.first); 
//                        levelFreqs[3].push_back(bucket*a3.second[0]*a3.second[1]); 
//                    }
// 
//                }
//            }
//        }
//    }
//    
//    end = std::chrono::system_clock::now();
//    elapsed = end - start;
//    std::cout << "\n sequential GFJS generation in memory: " << elapsed.count() << "s"<<endl;    
//    start = std::chrono::system_clock::now();
////    
////    
////    // inserting the summary to the disk seq
//
//    for(int f=0;f<levelFreqs.size();f++){
//        string fileName="/media/ali/2TB/output/"+to_string(f)+".txt";
//        ofstream output_file(fileName);
//        ostream_iterator<long long int> output_iterator(output_file, "|");
//        copy(levelFreqs[f].begin(), levelFreqs[f].end(), output_iterator);
//        output_file.close();
//    }
//       
//    end = std::chrono::system_clock::now();
//    elapsed = end - start;
//    std::cout << "\n sequential storing  in: " << elapsed.count() << "s"<<endl;    
//    start = std::chrono::system_clock::now();
//    
//
//    
//    //load and de-summarize seq
//      max_vec_initialization=200000000;
//    for(int f=0;f<levelFreqs.size();f++){
//        string fileName="/media/ali/2TB/output/"+to_string(f)+".txt";
//        std::ifstream file(fileName);
//        if (!file.good())
//            continue;
//        string line = "";
//        getline(file, line);
//        string::const_iterator start = line.begin();
//        string::const_iterator end = line.end();
//        string::const_iterator next = std::find(start, end, '|');
//        int key;
//        unsigned long long int value;
//        while (next != end)
//        {
//            key = stoi(string(start, next));
//            start = next + 1;
//            next = find(start, end, '|');
//            value=stoll(string(start, next));
//            start = next + 1;
//            next = find(start, end, '|');
//
//
//            if(value<max_vec_initialization){
//                vector <int> tmp;
//                tmp.assign(value, key);
//            }
//            else{
//                for(int i=0;i<value / max_vec_initialization;i++){
//                    vector <int> tmp;
//                    tmp.assign(max_vec_initialization, key);
//
//                }
//                vector <int> tmp;
//                tmp.assign(value % max_vec_initialization, key); // remaining
//            }
//
//        }
//        cout<< "Att "<<f<< " loaded."<<endl;
//    }
//    
//    end = std::chrono::system_clock::now();
//    elapsed = end - start;
//    std::cout << "\n sequential desummarization in: " << elapsed.count() << "s"<<endl;    
//    start = std::chrono::system_clock::now();
}

void do_parallel_lastFM_A1_linear(vector<vector<string>> &ua, vector<vector<string>> &uf,vector<vector<string>> &ua1, int numThreads){
    
    //      the graph:      w <-- u <--u1 <-- w1
    
    auto start = std::chrono::system_clock::now();
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed; 
    
    vector<unordered_map<unsigned int, unsigned long long int>> childMapL1; // empty
    
    vector<unordered_map<unsigned int, unsigned long long int>> w1_u1; // childs for the upper level
    unordered_map<unsigned int, unsigned long long int> cm1;
    w1_u1.push_back(cm1);
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u1_w1_gen;
    vector<unordered_map<unsigned int, unsigned long long int>> u1_u;
    unordered_map<unsigned int, unsigned long long int> cm2;
    u1_u.push_back(cm2);
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u_u1_gen;
   vector<unordered_map<unsigned int, unsigned long long int>> u_w;
    unordered_map<unsigned int, unsigned long long int> cm3;
    u_w.push_back(cm3);
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  w_u_gen;
    
 
//    omp_set_dynamic(10);
    omp_set_nested(2);
    omp_set_num_threads(numThreads);
    
    
    #pragma omp parallel
    {
        if( omp_get_num_threads()==0)
             printf("Number of threads = %d\n", omp_get_num_threads());
    #pragma omp single 
        {
            #pragma omp task
                parallelSum_produc(ua1, 0,w1_u1[0], u1_w1_gen,childMapL1);
            #pragma omp taskwait

            #pragma omp task 
                parallelSum_produc(uf, 0,u1_u[0], u_u1_gen,w1_u1);
            #pragma omp taskwait

            #pragma omp task
                parallelSum_produc(ua, 0,u_w[0], w_u_gen,u1_u);
            #pragma omp taskwait
        }

    }

    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n parallel inference in: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
//    //Result generation in the mem and throw it away
    long int max_vec_initialization=200000000;
    vector<vector<vector<unsigned long long int>>> levelData(numThreads,vector<vector<unsigned long long int>>(4));
//    
   #pragma omp parallel for  
    for(int i = 0; i < u_w[0].size(); i++) {
        auto root = u_w[0].begin();
        advance(root, i);
        int th_num=omp_get_thread_num();
          
        if((*root).second<max_vec_initialization){
            levelData[th_num][0].assign((*root).second,(*root).first ); 
        }
        else{
              for(int i=0;i<(*root).second/ max_vec_initialization;i++)
                  levelData[th_num][0].assign(max_vec_initialization,(*root).first );
              levelData[th_num][0].assign((*root).second% max_vec_initialization,(*root).first );
        }
        
        for (auto &u:w_u_gen[(*root).first]){
            auto value=u.second[0]*u.second[1];
            if(value<max_vec_initialization){
                levelData[th_num][1].assign(value,u.first); 
            }
            else{
                  for(int i=0;i<value / max_vec_initialization;i++){
                      levelData[th_num][1].assign(max_vec_initialization,u.first);
                  }
                  levelData[th_num][1].assign(value% max_vec_initialization,u.first);
            }
            
            for (auto &u1:u_u1_gen[u.first]){
                auto bucket=u.second[0]* u1.second[0];
                if(bucket*u1.second[1]<max_vec_initialization){
                    levelData[th_num][2].assign(bucket*u1.second[1],u1.first); 
                }
                else{
                      for(int i=0;i<bucket*u1.second[1] / max_vec_initialization;i++){
                          levelData[th_num][2].assign(max_vec_initialization,u1.first);
                      }
                      levelData[th_num][2].assign(bucket*u1.second[1]% max_vec_initialization,u1.first);
                }
                
                for (auto &w1:u1_w1_gen[u1.first]){
                    auto bucket1=bucket* w1.second[0];
                    if(bucket*w1.second[1]<max_vec_initialization){
                        levelData[th_num][3].assign(bucket1*w1.second[1],w1.first); 
                    }
                    else{
                          for(int i=0;i<bucket*w1.second[1] / max_vec_initialization;i++){
                              levelData[th_num][3].assign(max_vec_initialization,w1.first);
                          }
                          levelData[th_num][3].assign(bucket1*w1.second[1]% max_vec_initialization,w1.first);
                    }

                }
                
            }
            
        }
        
       
    }
    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n parallel generation in memory: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
    
    ////////////////////////
    

       //Summary generation and inserting to the disk
    
    
    vector<vector<vector<unsigned long long int>>> levelFreqs(numThreads,vector<vector<unsigned long long int>>(4));
    omp_set_num_threads(numThreads);
    #pragma omp parallel for 
    for(int i = 0; i < u_w[0].size(); i++) {
        int th_num= omp_get_thread_num();
        auto root = u_w[0].begin();
        advance(root, i); 
        levelFreqs[th_num][0].push_back((*root).first ); 
        levelFreqs[th_num][0].push_back((*root).second ); 

        for (auto &u:w_u_gen[(*root).first]){
            levelFreqs[th_num][1].push_back(u.first);
            levelFreqs[th_num][1].push_back(u.second[0]*u.second[1]);
            for (auto &u1:u_u1_gen[u.first]){
                auto bucket=u.second[0]*u1.second[0];
                levelFreqs[th_num][2].push_back(u1.first);
                levelFreqs[th_num][2].push_back(bucket*u1.second[1]);
                
                for (auto &w1:u1_w1_gen[u1.first]){
                    auto bucket1=bucket*w1.second[0];
                    levelFreqs[th_num][3].push_back(w1.first);
                    levelFreqs[th_num][3].push_back(bucket1*w1.second[1]);
                }
            }
            
        }
        
    }

    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n parallel gfjs generation in memory: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
    // inserting the summary to the disk
    #pragma omp parallel
    {
        int th_num= omp_get_thread_num();
//        cout<<th_num<<endl;
        for(int f=0;f<levelFreqs[th_num].size();f++){
            string fileName="/media/ali/2TB/output/"+to_string(th_num)+"_"+to_string(f)+".csv";
            ofstream output_file(fileName);
            ostream_iterator<long long int> output_iterator(output_file, "|");
            copy(levelFreqs[th_num][f].begin(), levelFreqs[th_num][f].end(), output_iterator);
            output_file.close();
        }
    }
       
    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n parallel storing: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
    //load and de-summarize in parallel
     //load and de-summarize seq
    max_vec_initialization=200000000;
    #pragma omp parallel
    {
        int th_num= omp_get_thread_num();
        for(int f=0;f<levelFreqs[th_num].size();f++){
            string fileName="/media/ali/2TB/output/"+to_string(th_num)+"_"+to_string(f)+".csv";
            std::ifstream file(fileName);
            if (!file.good())
            {
                cout<<"file not exists"<<endl;
                exit(1);
            }
            string line = "";
            getline(file, line);
            string::const_iterator start = line.begin();
            string::const_iterator end = line.end();
            string::const_iterator next = std::find(start, end, '|');
            int key;
            unsigned long long int value;
            while (next != end)
            {
                key = stoi(string(start, next));
                start = next + 1;
                next = find(start, end, '|');
                value=stoll(string(start, next));
                start = next + 1;
                next = find(start, end, '|');


                if(value<max_vec_initialization){
                    vector <int> tmp;
                    tmp.assign(value, key);
                }
                else{
                    for(int i=0;i<value / max_vec_initialization;i++){
                        vector <int> tmp;
                        tmp.assign(max_vec_initialization, key);

                    }
                    vector <int> tmp;
                    tmp.assign(value % max_vec_initialization, key); // remaining
                }

            }
//            cout<< fileName<<endl;
        }
    }
    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n parallel loading in: " << elapsed.count() << "s"<<endl;    
//   
    
}



void do_parallel_lastFM_A1(vector<vector<string>> &ua, vector<vector<string>> &uf,vector<vector<string>> &ua1, int numThreads){
    
    //      the graph:      w --> u <--u1 <-- w1
    
    auto start = std::chrono::system_clock::now();
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed; 
    
    vector<unordered_map<unsigned int, unsigned long long int>> childMapL1; // empty
    
    vector<unordered_map<unsigned int, unsigned long long int>> w2u; // childs for the upper level
    unordered_map<unsigned int, unsigned long long int> cm1;
    w2u.push_back(cm1);
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u2wGrn;
    vector<unordered_map<unsigned int, unsigned long long int>> w1_2_u1;
    unordered_map<unsigned int, unsigned long long int> cm2;
    w1_2_u1.push_back(cm2);
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u1_to_w1Gen;
    unordered_map<unsigned int, unsigned long long int> cm3;
    w2u.push_back(cm3);
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  u_to_u1Gen;
    
 
//    omp_set_dynamic(10);
    omp_set_nested(2);
    omp_set_num_threads(numThreads);
    
    
    #pragma omp parallel
    {
        if( omp_get_num_threads()==0)
             printf("Number of threads = %d\n", omp_get_num_threads());
    #pragma omp single nowait
        {
            #pragma omp task 
                parallelSum_produc(ua, 0,w2u[0], u2wGrn,childMapL1);
            #pragma omp task
                parallelSum_produc(ua1, 0,w1_2_u1[0], u1_to_w1Gen,childMapL1);
            #pragma omp taskwait
            #pragma omp task
                parallelSum_produc(uf, 0,w2u[1], u_to_u1Gen,w1_2_u1);

        }

    }

    
    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n parallel inference in: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    long long int coun=0;
    for(auto &x:w2u[0])
        if(w2u[1].find(x.first)!=w2u[1].end())
            coun+=x.second * w2u[1][x.first];
    cout<<"Join size is: "<<coun<<endl;
    
//    //Result generation in the mem and throw it away
//    long int max_vec_initialization=200000000;
//    vector<vector<vector<unsigned long long int>>> levelData(numThreads,vector<vector<unsigned long long int>>(4));
////    
//   #pragma omp parallel for  
//    for(int i = 0; i < w2u[0].size(); i++) {
//        auto root = w2u[0].begin();
//        advance(root, i);
//        int th_num=omp_get_thread_num();
//        if(w2u[1].find((*root).first)!=w2u[1].end()){
//            auto rootVal=(*root).second*w2u[1][(*root).first];    
//            if(rootVal<max_vec_initialization){
//                levelData[th_num][0].assign(rootVal,(*root).first ); 
//            }
//            else{
//                  for(int i=0;i<rootVal/ max_vec_initialization;i++)
//                      levelData[th_num][0].assign(max_vec_initialization,(*root).first );
//                  levelData[th_num][0].assign(rootVal% max_vec_initialization,(*root).first );
//            }
//            
//            for(auto &w:u2wGrn[(*root).first]){
//                for(auto &u1:u_to_u1Gen[(*root).first]){
//                    unsigned long long int bucket=w.second[0]* u1.second[0];
//                    unsigned long long int value=bucket* w.second[1]*u1.second[1];
//                    if(value<max_vec_initialization){
//                        levelData[th_num][1].assign(value,w.first); 
//                        levelData[th_num][2].assign(value,u1.first); 
//                    }
//                    else{
//                          for(int i=0;i<value / max_vec_initialization;i++){
//                              levelData[th_num][1].assign(max_vec_initialization,w.first);
//                              levelData[th_num][2].assign(max_vec_initialization,u1.first);
//                          }
//                          levelData[th_num][1].assign(value% max_vec_initialization,w.first);
//                          levelData[th_num][2].assign(value% max_vec_initialization,u1.first);
//                    }
//                    
//                    for (auto &w1:u1_to_w1Gen[u1.first]){
//                        unsigned long long int value=bucket* w1.second[0]*w1.second[1];
//                        if(value<max_vec_initialization){
//                            levelData[th_num][3].assign(value,w1.first); 
//                        }
//                        else{
//                              for(int i=0;i<value / max_vec_initialization;i++){
//                                  levelData[th_num][3].assign(max_vec_initialization,w1.first);
//                              }
//                              levelData[th_num][3].assign(value% max_vec_initialization,w1.first);
//                        }
//                        
//                        //If you wanna print all the tuples
////                        cout<<w.first<<",  "<<(*root).first<<",  "<<u1.first<<",  "<<w1.first<<", freq:  "<<value<<endl;
//                    }
//                    
//                    
//                }
//            }
//        }
//    }
//   end = std::chrono::system_clock::now();
//    elapsed = end - start;
//    std::cout << "\n parallel generation in memory: " << elapsed.count() << "s"<<endl;    
//    start = std::chrono::system_clock::now();
//    
//    
//    ////////////////////////
//    
//
//       //Summary generation and inserting to the disk
//    
//    
//    vector<vector<vector<unsigned long long int>>> levelFreqs(numThreads,vector<vector<unsigned long long int>>(4));
//    omp_set_num_threads(numThreads);
//    #pragma omp parallel for 
//    for(int i = 0; i < w2u[0].size(); i++) {
//        int th_num= omp_get_thread_num();
//        auto root = w2u[0].begin();
//        advance(root, i);
//        if(w2u[1].find((*root).first)!=w2u[1].end()){   
//            levelFreqs[th_num][0].push_back((*root).first ); 
//            levelFreqs[th_num][0].push_back((*root).second*w2u[1][(*root).first] ); 
//   
//            for(auto &w:u2wGrn[(*root).first]){
//                for(auto &u1:u_to_u1Gen[(*root).first]){
//                    unsigned long long int bucket=w.second[0]* u1.second[0];
//                    unsigned long long int value=bucket* w.second[1]*u1.second[1];
//                    levelFreqs[th_num][1].push_back(w.first); 
//                    levelFreqs[th_num][1].push_back(value); 
//                    levelFreqs[th_num][2].push_back(u1.first); 
//                    levelFreqs[th_num][2].push_back(value); 
//                    
//                    for (auto &w1:u1_to_w1Gen[u1.first]){
//                        levelFreqs[th_num][3].push_back(w1.first);
//                        levelFreqs[th_num][3].push_back(bucket* w1.second[0]*w1.second[1]);
//  
//                        //If you wanna print all the tuples
////                        cout<<w.first<<",  "<<(*root).first<<",  "<<u1.first<<",  "<<w1.first<<", freq:  "<<value<<endl;
//                    }
//                    
//                    //***********************************  
//                    //********************************
//                    // Why GFJS generation is slower than full join result generation in memory?????????????????????????????????
//                   
//                }
//            }
//        }
//    }
//
//    end = std::chrono::system_clock::now();
//    elapsed = end - start;
//    std::cout << "\n parallel gfjs generation in memory: " << elapsed.count() << "s"<<endl;    
//    start = std::chrono::system_clock::now();
//    
//    // inserting the summary to the disk
//    #pragma omp parallel
//    {
//        int th_num= omp_get_thread_num();
////        cout<<th_num<<endl;
//        for(int f=0;f<levelFreqs[th_num].size();f++){
//            string fileName="/media/ali/2TB/output/"+to_string(th_num)+"_"+to_string(f)+".csv";
//            ofstream output_file(fileName);
//            ostream_iterator<long long int> output_iterator(output_file, "|");
//            copy(levelFreqs[th_num][f].begin(), levelFreqs[th_num][f].end(), output_iterator);
//            output_file.close();
//        }
//    }
//       
//    end = std::chrono::system_clock::now();
//    elapsed = end - start;
//    std::cout << "\n parallel storing: " << elapsed.count() << "s"<<endl;    
//    start = std::chrono::system_clock::now();
//    
//    //load and de-summarize in parallel
//     //load and de-summarize seq
//    max_vec_initialization=200000000;
//    #pragma omp parallel
//    {
//        int th_num= omp_get_thread_num();
//        for(int f=0;f<levelFreqs[th_num].size();f++){
//            string fileName="/media/ali/2TB/output/"+to_string(th_num)+"_"+to_string(f)+".csv";
//            std::ifstream file(fileName);
//            if (!file.good())
//            {
//                cout<<"file not exists"<<endl;
//                exit(1);
//            }
//            string line = "";
//            getline(file, line);
//            string::const_iterator start = line.begin();
//            string::const_iterator end = line.end();
//            string::const_iterator next = std::find(start, end, '|');
//            int key;
//            unsigned long long int value;
//            while (next != end)
//            {
//                key = stoi(string(start, next));
//                start = next + 1;
//                next = find(start, end, '|');
//                value=stoll(string(start, next));
//                start = next + 1;
//                next = find(start, end, '|');
//
//
//                if(value<max_vec_initialization){
//                    vector <int> tmp;
//                    tmp.assign(value, key);
//                }
//                else{
//                    for(int i=0;i<value / max_vec_initialization;i++){
//                        vector <int> tmp;
//                        tmp.assign(max_vec_initialization, key);
//
//                    }
//                    vector <int> tmp;
//                    tmp.assign(value % max_vec_initialization, key); // remaining
//                }
//
//            }
////            cout<< fileName<<endl;
//        }
//    }
//    end = std::chrono::system_clock::now();
//    elapsed = end - start;
//    std::cout << "\n parallel loading in: " << elapsed.count() << "s"<<endl;    
//   
    
}




void do_seq_test(vector<vector<string>> &rawData1, vector<vector<string>> &rawData2,vector<vector<string>> &rawData3){
    auto start = std::chrono::system_clock::now();
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed; 
    
    vector<unordered_map<unsigned int, unsigned long long int>> childMapL1; // empty
    
    vector<unordered_map<unsigned int, unsigned long long int>> childMapL2; // childs for the upper level
    unordered_map<unsigned int, unsigned long long int> cm1;
    childMapL2.push_back(cm1);
    
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  L1_GFJS1;
    sequentialSum_produc(rawData1, 0,childMapL2[0], L1_GFJS1,childMapL1);
    
    unordered_map<unsigned int, unsigned long long int> cm2;
    childMapL2.push_back(cm2);
    
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  L1_GFJS2;
    sequentialSum_produc(rawData2, 0,childMapL2[1], L1_GFJS2,childMapL1);
    
    vector<unordered_map<unsigned int, unsigned long long int>> childMapL3; // childs for the upper level
    unordered_map<unsigned int, unsigned long long int> cm3;
    childMapL3.push_back(cm3);
    
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  L2_GFJS1;
    sequentialSum_produc(rawData3, 0,childMapL3[0], L2_GFJS1,childMapL2);
    
    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n sequential inference in: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
    long long int coun=0;
    for(auto &x:childMapL3[0])
        coun+=x.second;
    cout<<"Join size is: "<<coun<<endl;
    
//    //start result generation in memory and throw it away 
    
//    long int max_vec_initialization=200000000;
//    vector<vector<unsigned long long int>> levelFreqs(4);
//    for(auto &root:childMapL3[0]){
////        cout<<root.first<<", "<<root.second<<endl;
//        if(root.second<max_vec_initialization){
//            levelFreqs[0].assign(root.second,root.first ); 
//        }
//        else{
//              for(int i=0;i<root.second / max_vec_initialization;i++)
//                  levelFreqs[0].assign(max_vec_initialization,root.first );
//              levelFreqs[0].assign(root.second% max_vec_initialization,root.first );
//        }
//        
//        
//        for(auto &L2:L2_GFJS1[root.first]){
////            cout<<"\t"<<L2.first<<", "<<L2.second[0]*L2.second[1]<<endl;
//            unsigned long long int value=L2.second[0]*L2.second[1];
//            if(value<max_vec_initialization){
//                levelFreqs[1].assign(value,L2.first ); 
//            }
//            else{
//                  for(int i=0;i<value / max_vec_initialization;i++)
//                      levelFreqs[1].assign(max_vec_initialization,L2.first);
//                  levelFreqs[1].assign(value% max_vec_initialization,L2.first );
//            }
//            
//            for(auto &L11:L1_GFJS1[L2.first])
//                for(auto &L12:L1_GFJS2[L2.first]){
////                    unsigned long long int bucket=L2.second[0]   * L11.second[0]* L12.second[0];
////                    cout<<"\t\t"<<L11.first<<", "<<L12.first<<", "<<L2.second[0]* L11.second[0]* L12.second[0]* L11.second[1]*L12.second[1]<<endl;
//                    unsigned long long int value=L2.second[0]* L11.second[0]* L12.second[0]* L11.second[1]*L12.second[1];
//                    if(value<max_vec_initialization){
//                        levelFreqs[2].assign(value,L11.first); 
//                        levelFreqs[3].assign(value,L12.first); 
//                    }
//                    else{
//                          for(int i=0;i<value / max_vec_initialization;i++){
//                              levelFreqs[2].assign(max_vec_initialization,L11.first);
//                              levelFreqs[3].assign(max_vec_initialization,L12.first);
//                          }
//                          levelFreqs[2].assign(value% max_vec_initialization,L11.first);
//                          levelFreqs[3].assign(value% max_vec_initialization,L12.first);
//                    }
//                }
//        }
//    }
 
 
  
    
    
// true generation with keeping results in memory   //start result generation
    
//    long int max_vec_initialization=200000000;
//    vector<vector<unsigned long long int>> levelFreqs(4);
//    for(auto &root:childMapL3[0]){
////        cout<<root.first<<", "<<root.second<<endl;
//        if(root.second<max_vec_initialization){
////            levelFreqs[0].assign(root.second,root.first ); 
//            levelFreqs[0].insert(levelFreqs[0].end(),root.second,root.first ); 
//           
//        }
//        else{
//              for(int i=0;i<root.second / max_vec_initialization;i++)
//                  levelFreqs[0].insert(levelFreqs[0].end(),max_vec_initialization,root.first );
//              levelFreqs[0].insert(levelFreqs[0].end(),root.second% max_vec_initialization,root.first );
//        }
//        
//        
//        for(auto &L2:L2_GFJS1[root.first]){
////            cout<<"\t"<<L2.first<<", "<<L2.second[0]*L2.second[1]<<endl;
//            unsigned long long int value=L2.second[0]*L2.second[1];
//            if(value<max_vec_initialization){
//                levelFreqs[1].insert(levelFreqs[1].end(),value,L2.first ); 
//            }
//            else{
//                  for(int i=0;i<value / max_vec_initialization;i++)
//                      levelFreqs[1].insert(levelFreqs[1].end(),max_vec_initialization,L2.first);
//                  levelFreqs[1].insert(levelFreqs[1].end(),value% max_vec_initialization,L2.first );
//            }
//            
//            for(auto &L11:L1_GFJS1[L2.first])
//                for(auto &L12:L1_GFJS2[L2.first]){
////                    unsigned long long int bucket=L2.second[0]   * L11.second[0]* L12.second[0];
////                    cout<<"\t\t"<<L11.first<<", "<<L12.first<<", "<<L2.second[0]* L11.second[0]* L12.second[0]* L11.second[1]*L12.second[1]<<endl;
//                    unsigned long long int value=L2.second[0]* L11.second[0]* L12.second[0]* L11.second[1]*L12.second[1];
//                    if(value<max_vec_initialization){
//                        levelFreqs[2].insert(levelFreqs[2].end(),value,L11.first); 
//                        levelFreqs[3].insert(levelFreqs[3].end(),value,L12.first); 
//                    }
//                    else{
//                          for(int i=0;i<value / max_vec_initialization;i++){
//                              levelFreqs[2].insert(levelFreqs[2].end(),max_vec_initialization,L11.first);
//                              levelFreqs[3].insert(levelFreqs[3].end(),max_vec_initialization,L12.first);
//                          }
//                          levelFreqs[2].insert(levelFreqs[2].end(),value% max_vec_initialization,L11.first);
//                          levelFreqs[3].insert(levelFreqs[3].end(),value% max_vec_initialization,L12.first);
//                    }
//                }
//        }
//    }
// 
//    for(int i=0;i<levelFreqs[0].size();i++){
//        for(int j=0;j<levelFreqs.size();j++)
//            cout<<levelFreqs[j][i]<<", ";
//        cout<<endl;
//    }
    
    
    
    
    
    ///////////////////
    
    
    
    
    
    
    
    
    
    //Summary generation and inserting to the disk
    
    
    vector<vector<unsigned long long int>> levelFreqs(4);
    for(auto &root:childMapL3[0]){
            levelFreqs[0].push_back(root.first); 
            levelFreqs[0].push_back(root.second ); 

        for(auto &L2:L2_GFJS1[root.first]){
            levelFreqs[1].push_back(L2.first); 
            levelFreqs[1].push_back(L2.second[0]*L2.second[1]); 
            
            for(auto &L11:L1_GFJS1[L2.first])
                for(auto &L12:L1_GFJS2[L2.first]){
//                    unsigned long long int bucket=L2.second[0]   * L11.second[0]* L12.second[0];
                    unsigned long long int value=L2.second[0]* L11.second[0]* L12.second[0]* L11.second[1]*L12.second[1];
                    levelFreqs[2].push_back(L11.first); 
                    levelFreqs[2].push_back(value); 
                    levelFreqs[3].push_back(L12.first); 
                    levelFreqs[3].push_back(value); 
                   
            }
        }
    }
    
    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n sequential GFJS generation in memory: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
    // inserting the summary to the disk seq

    for(int f=0;f<levelFreqs.size();f++){
        string fileName="/media/ali/2TB/output/"+to_string(f)+".csv";
        ofstream output_file(fileName);
        ostream_iterator<long long int> output_iterator(output_file, "|");
        copy(levelFreqs[f].begin(), levelFreqs[f].end(), output_iterator);
        output_file.close();
    }
       
    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n sequential storing  in: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
    //load and de-summarize seq
    long int max_vec_initialization=200000000;
    for(int f=0;f<levelFreqs.size();f++){
        string fileName="/media/ali/2TB/output/"+to_string(f)+".csv";
        std::ifstream file(fileName);
        if (!file.good())
            continue;
        string line = "";
        getline(file, line);
        string::const_iterator start = line.begin();
        string::const_iterator end = line.end();
        string::const_iterator next = std::find(start, end, '|');
        int key;
        unsigned long long int value;
        while (next != end)
        {
            key = stoi(string(start, next));
            start = next + 1;
            next = find(start, end, '|');
            value=stoll(string(start, next));
            start = next + 1;
            next = find(start, end, '|');


            if(value<max_vec_initialization){
                vector <int> tmp;
                tmp.assign(value, key);
            }
            else{
                for(int i=0;i<value / max_vec_initialization;i++){
                    vector <int> tmp;
                    tmp.assign(max_vec_initialization, key);

                }
                vector <int> tmp;
                tmp.assign(value % max_vec_initialization, key); // remaining
            }

        }
//        cout<< "Att "<<f<< " loaded."<<endl;
    }
    
    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n sequential desummarization in: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
}
void do_parallel_test(vector<vector<string>> &rawData1, vector<vector<string>> &rawData2,vector<vector<string>> &rawData3, int numThreads){
    
    auto start = std::chrono::system_clock::now();
    auto end = std::chrono::system_clock::now();
    std::chrono::duration<double> elapsed; 
    
    vector<unordered_map<unsigned int, unsigned long long int>> childMapL1; // empty
    
    vector<unordered_map<unsigned int, unsigned long long int>> childMapL2; // childs for the upper level
    unordered_map<unsigned int, unsigned long long int> cm1;
    childMapL2.push_back(cm1);
    
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  L1_GFJS1;
    
    
    unordered_map<unsigned int, unsigned long long int> cm2;
    childMapL2.push_back(cm2);
    
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  L1_GFJS2;
     
    
    vector<unordered_map<unsigned int, unsigned long long int>> childMapL3; // childs for the upper level
    unordered_map<unsigned int, unsigned long long int> cm3;
    childMapL3.push_back(cm3);
    
    unordered_map <unsigned int, vector<pair<unsigned int, vector<unsigned long long int>> > >  L2_GFJS1;
 
//    omp_set_dynamic(10);
    omp_set_nested(2);
    omp_set_num_threads(numThreads);
    
    #pragma omp parallel
    {
        if( omp_get_num_threads()==0)
             printf("Number of threads = %d\n", omp_get_num_threads());
    #pragma omp single nowait
        {
            #pragma omp task 
                parallelSum_produc(rawData1, 0,childMapL2[0], L1_GFJS1,childMapL1);
            #pragma omp task
                parallelSum_produc(rawData2, 0,childMapL2[1], L1_GFJS2,childMapL1);
            #pragma omp taskwait
            #pragma omp task
                parallelSum_produc(rawData3, 0,childMapL3[0], L2_GFJS1,childMapL2);

        }

    }

    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n parallel inference in: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
//    //Result generation in the mem and throw it away
//    long int max_vec_initialization=200000000;
//    vector<vector<vector<unsigned long long int>>> levelFreqs(numThreads,vector<vector<unsigned long long int>>(4));
//    
//    #pragma omp parallel for  
//    for(int i = 0; i < childMapL3[0].size(); i++) {
//        auto root = childMapL3[0].begin();
//        advance(root, i);
////        cout<<(*root).first<<", "<<(*root).second<<endl;
//        if((*root).second<max_vec_initialization){
//            levelFreqs[omp_get_thread_num()][0].assign((*root).second,(*root).first ); 
//        }
//        else{
//              for(int i=0;i<(*root).second / max_vec_initialization;i++)
//                  levelFreqs[omp_get_thread_num()][0].assign(max_vec_initialization,(*root).first );
//              levelFreqs[omp_get_thread_num()][0].assign((*root).second% max_vec_initialization,(*root).first );
//        }
//        
//        
//        for(auto &L2:L2_GFJS1[(*root).first]){
////            cout<<"\t"<<L2.first<<", "<<L2.second[0]*L2.second[1]<<endl;
//            unsigned long long int value=L2.second[0]*L2.second[1];
//            if(value<max_vec_initialization){
//                levelFreqs[omp_get_thread_num()][1].assign(value,L2.first ); 
//            }
//            else{
//                  for(int i=0;i<value / max_vec_initialization;i++)
//                      levelFreqs[omp_get_thread_num()][1].assign(max_vec_initialization,L2.first);
//                  levelFreqs[omp_get_thread_num()][1].assign(value% max_vec_initialization,L2.first );
//            }
//            
//            for(auto &L11:L1_GFJS1[L2.first])
//                for(auto &L12:L1_GFJS2[L2.first]){
////                    unsigned long long int bucket=L2.second[0]   * L11.second[0]* L12.second[0];
////                    cout<<"\t\t"<<L11.first<<", "<<L12.first<<", "<<L2.second[0]* L11.second[0]* L12.second[0]* L11.second[1]*L12.second[1]<<endl;
//                    unsigned long long int value=L2.second[0]* L11.second[0]* L12.second[0]* L11.second[1]*L12.second[1];
//                    if(value<max_vec_initialization){
//                        levelFreqs[omp_get_thread_num()][2].assign(value,L11.first); 
//                        levelFreqs[omp_get_thread_num()][3].assign(value,L12.first); 
//                    }
//                    else{
//                          for(int i=0;i<value / max_vec_initialization;i++){
//                              levelFreqs[omp_get_thread_num()][2].assign(max_vec_initialization,L11.first);
//                              levelFreqs[omp_get_thread_num()][3].assign(max_vec_initialization,L12.first);
//                          }
//                          levelFreqs[omp_get_thread_num()][2].assign(value% max_vec_initialization,L11.first);
//                          levelFreqs[omp_get_thread_num()][3].assign(value% max_vec_initialization,L12.first);
//                    }
//                }
//        }
//    }
 
   
    
    
    ////////////////////////
    
    
    
    
    
    
    
// true genertion with keeping results in memory    //start GFJS generation
//    long int max_vec_initialization=200000000;
//    vector<vector<vector<unsigned long long int>>> levelFreqs(numThreads,vector<vector<unsigned long long int>>(4));
//    
//    #pragma omp parallel for  
//    for(int i = 0; i < childMapL3[0].size(); i++) {
//        auto root = childMapL3[0].begin();
//        advance(root, i);
//        cout<<(*root).first<<", "<<(*root).second<<endl;
//        if((*root).second<max_vec_initialization){
//            levelFreqs[omp_get_thread_num()][0].insert(levelFreqs[omp_get_thread_num()][0].end(),(*root).second,(*root).first ); 
//        }
//        else{
//              for(int i=0;i<(*root).second / max_vec_initialization;i++)
//                  levelFreqs[omp_get_thread_num()][0].insert(levelFreqs[omp_get_thread_num()][0].end(),max_vec_initialization,(*root).first );
//              levelFreqs[omp_get_thread_num()][0].insert(levelFreqs[omp_get_thread_num()][0].end(),(*root).second% max_vec_initialization,(*root).first );
//        }
//        
//        
//        for(auto &L2:L2_GFJS1[(*root).first]){
//            cout<<"\t"<<L2.first<<", "<<L2.second[0]*L2.second[1]<<endl;
//            unsigned long long int value=L2.second[0]*L2.second[1];
//            if(value<max_vec_initialization){
//                levelFreqs[omp_get_thread_num()][1].insert(levelFreqs[omp_get_thread_num()][1].end(),value,L2.first ); 
//            }
//            else{
//                  for(int i=0;i<value / max_vec_initialization;i++)
//                      levelFreqs[omp_get_thread_num()][1].insert(levelFreqs[omp_get_thread_num()][1].end(),max_vec_initialization,L2.first);
//                  levelFreqs[omp_get_thread_num()][1].insert(levelFreqs[omp_get_thread_num()][1].end(),value% max_vec_initialization,L2.first );
//            }
//            
//            for(auto &L11:L1_GFJS1[L2.first])
//                for(auto &L12:L1_GFJS2[L2.first]){
////                    unsigned long long int bucket=L2.second[0]   * L11.second[0]* L12.second[0];
//                    cout<<"\t\t"<<L11.first<<", "<<L12.first<<", "<<L2.second[0]* L11.second[0]* L12.second[0]* L11.second[1]*L12.second[1]<<endl;
//                    unsigned long long int value=L2.second[0]* L11.second[0]* L12.second[0]* L11.second[1]*L12.second[1];
//                    if(value<max_vec_initialization){
//                        levelFreqs[omp_get_thread_num()][2].insert(levelFreqs[omp_get_thread_num()][2].end(),value,L11.first); 
//                        levelFreqs[omp_get_thread_num()][3].insert(levelFreqs[omp_get_thread_num()][3].end(),value,L12.first); 
//                    }
//                    else{
//                          for(int i=0;i<value / max_vec_initialization;i++){
//                              levelFreqs[omp_get_thread_num()][2].insert(levelFreqs[omp_get_thread_num()][2].end(),max_vec_initialization,L11.first);
//                              levelFreqs[omp_get_thread_num()][3].insert(levelFreqs[omp_get_thread_num()][3].end(),max_vec_initialization,L12.first);
//                          }
//                          levelFreqs[omp_get_thread_num()][2].insert(levelFreqs[omp_get_thread_num()][2].end(),value% max_vec_initialization,L11.first);
//                          levelFreqs[omp_get_thread_num()][3].insert(levelFreqs[omp_get_thread_num()][3].end(),value% max_vec_initialization,L12.first);
//                    }
//                }
//        }
//    }
    
//    
    
    
    
    ///////////////////////////////////////////
    
    
    
    
    
     //gfjs generation in the mem 
    vector<vector<vector<unsigned long long int>>> levelFreqs(numThreads,vector<vector<unsigned long long int>>(4));
    omp_set_num_threads(numThreads);
    #pragma omp parallel for  
    for(int i = 0; i < childMapL3[0].size(); i++) {
        int th_num= omp_get_thread_num();
//        cout<<th_num<<endl;
        auto root = childMapL3[0].begin();
        advance(root, i);
        
        levelFreqs[th_num][0].push_back((*root).first ); 
        levelFreqs[th_num][0].push_back((*root).second ); 
        
        for(auto &L2:L2_GFJS1[(*root).first]){
            levelFreqs[th_num][1].push_back(L2.first ); 
            levelFreqs[th_num][1].push_back(L2.second[0]*L2.second[1]); 
            
            for(auto &L11:L1_GFJS1[L2.first])
                for(auto &L12:L1_GFJS2[L2.first]){
                    unsigned long long int value=L2.second[0]* L11.second[0]* L12.second[0]* L11.second[1]*L12.second[1];
                    levelFreqs[th_num][2].push_back(L11.first); 
                    levelFreqs[th_num][2].push_back(value); 
                    levelFreqs[th_num][3].push_back(L12.first); 
                    levelFreqs[th_num][3].push_back(value);
                }
        }
    }
 
    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n parallel gfjs generation in: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
    // inserting the summary to the disk
    #pragma omp parallel
    {
        int th_num= omp_get_thread_num();
//        cout<<th_num<<endl;
        for(int f=0;f<levelFreqs[th_num].size();f++){
            string fileName="/media/ali/2TB/output/"+to_string(th_num)+"_"+to_string(f)+".csv";
            ofstream output_file(fileName);
            ostream_iterator<long long int> output_iterator(output_file, "|");
            copy(levelFreqs[th_num][f].begin(), levelFreqs[th_num][f].end(), output_iterator);
            output_file.close();
        }
    }
       
    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n parallel storing: " << elapsed.count() << "s"<<endl;    
    start = std::chrono::system_clock::now();
    
    //load and de-summarize in parallel
     //load and de-summarize seq
    long int max_vec_initialization=200000000;
    #pragma omp parallel
    {
        int th_num= omp_get_thread_num();
        for(int f=0;f<levelFreqs[th_num].size();f++){
            string fileName="/media/ali/2TB/output/"+to_string(th_num)+"_"+to_string(f)+".csv";
            std::ifstream file(fileName);
            if (!file.good())
            {
                cout<<"file not exists"<<endl;
                exit(1);
            }
            string line = "";
            getline(file, line);
            string::const_iterator start = line.begin();
            string::const_iterator end = line.end();
            string::const_iterator next = std::find(start, end, '|');
            int key;
            unsigned long long int value;
            while (next != end)
            {
                key = stoi(string(start, next));
                start = next + 1;
                next = find(start, end, '|');
                value=stoll(string(start, next));
                start = next + 1;
                next = find(start, end, '|');


                if(value<max_vec_initialization){
                    vector <int> tmp;
                    tmp.assign(value, key);
                }
                else{
                    for(int i=0;i<value / max_vec_initialization;i++){
                        vector <int> tmp;
                        tmp.assign(max_vec_initialization, key);

                    }
                    vector <int> tmp;
                    tmp.assign(value % max_vec_initialization, key); // remaining
                }

            }
//            cout<< fileName<<endl;
        }
    }
    end = std::chrono::system_clock::now();
    elapsed = end - start;
    std::cout << "\n parallel loading in: " << elapsed.count() << "s"<<endl;    
//   
    
}

int main(int argc, char* argv[]){
    
    
    
//    int numThreads=6;
    
    long int tableSize1=100000; long int tableSize2=10000;long int tableSize3=10000; int firstVarSize=100; int secondVarSize=100;  int childMapSize=firstVarSize;  
    
//    generateSynthData(tableSize1,firstVarSize, secondVarSize, "/media/ali/2TB/output/synth1.csv");
//    generateSynthData(tableSize2,firstVarSize, secondVarSize, "/media/ali/2TB/output/synth2.csv");
//    generateSynthData(tableSize3,firstVarSize, secondVarSize, "/media/ali/2TB/output/synth3.csv");
    
    //// test query
    
//    string tableADD="/media/ali/2TB/output/synth1.csv";
//    vector<string> variableList={"att1", "att2"};
//    vector<vector<string>> rawData1; 
//    loadRawData(tableADD,variableList, rawData1, '|');
//
//    tableADD="/media/ali/2TB/output/synth2.csv";
//    vector<vector<string>> rawData2; 
//    loadRawData(tableADD,variableList, rawData2, '|');
//    
//    tableADD="/media/ali/2TB/output/synth3.csv";
//    vector<vector<string>> rawData3; 
//    loadRawData(tableADD,variableList, rawData3, '|');
//    cout<< "Loading the data finished"<<endl;
//    
//    
//    
//    auto start = std::chrono::system_clock::now();
//    auto end = std::chrono::system_clock::now();
//    std::chrono::duration<double> elapsed; 
//    
//    do_seq_test(rawData1,rawData2,rawData3);
//    end = std::chrono::system_clock::now();
//    elapsed = end - start;
//    std::cout << "\nElapsed wall time for sequential program with reduction: " << elapsed.count() << "s"<<endl;    
//    
//
//    for(int numThreads=1;numThreads<7;numThreads++){
//       
//        start = std::chrono::system_clock::now();
//        do_parallel_test(rawData1,rawData2,rawData3,numThreads);
//
//        end = std::chrono::system_clock::now();
//        elapsed = end - start;
//        std::cout << "\nElapsed wall time for nested-parallel program with reduction: " << elapsed.count() << "s"<<endl;
//
//    }
    
    
    
    ///////////////////////////////////////////////////////////////////////////////// lastFM_A1
    
//    string tableADD="/media/ali/2TB/data/lastFM/ua.csv";
//    vector<string> variableList={"userID", "weight"};
//    vector<vector<string>> ua; 
//    loadRawData(tableADD,variableList, ua, '|');
//    
//    variableList={"userID", "userID1"};
//    tableADD="/media/ali/2TB/data/lastFM/uf.csv";
//    vector<vector<string>> uf; 
//    loadRawData(tableADD,variableList, uf, '|');
//    
//    variableList={"userID1", "weight1"};
//    tableADD="/media/ali/2TB/data/lastFM/ua1.csv";
//    vector<vector<string>> ua1; 
//    loadRawData(tableADD,variableList, ua1, '|');
//    cout<< "Loading the data finished"<<endl;
//    
//    auto start = std::chrono::system_clock::now();
//    auto end = std::chrono::system_clock::now();
//    std::chrono::duration<double> elapsed;
//    
//    for(int i=0;i<1;i++){
//        cout<< "run:"<<i<<"------------------------------------------------------------------------"<<endl;
//        do_seq_lastFM_A1(1,ua,uf,ua1);
//    }
//    end = std::chrono::system_clock::now();
//    elapsed = end - start;
//    std::cout << "\nElapsed wall time for sequential program with reduction: " << elapsed.count() << "s"<<endl;    
//    
//
//    for(int numThreads=6;numThreads<7;numThreads++){
//       
//        start = std::chrono::system_clock::now();
//        for(int i=0;i<50;i++){
////            cout<< "run:"<<i<<"------------------------------------------------------------------------"<<endl;
//            do_parallel_lastFM_A1(ua,uf,ua1,numThreads);
//        }
//        end = std::chrono::system_clock::now();
//        elapsed = end - start;
//        std::cout << "\nElapsed wall time for nested-parallel program with reduction and num of threads:"<<numThreads<< "  is: " << elapsed.count()/50.0 << "s"<<endl;
//
//    }
    
//    for(int numThreads=1;numThreads<7;numThreads++){
//       
//        start = std::chrono::system_clock::now();
//        do_parallel_lastFM_A1_linear(ua,uf,ua1,numThreads);
//
//        end = std::chrono::system_clock::now();
//        elapsed = end - start;
//        std::cout << "\n -----------   (linear)  Elapsed wall time for nested-parallel program with reduction and num of threads:"<<numThreads<< "  is: " << elapsed.count() << "s"<<endl;
//
//    }
    
    
    
    
    //////////////////////////////////////////////////////////////////////////////////  lastFM_A2
    

    
//    
    string tableADD="/media/ali/2TB/data/lastFM/SF4/A2/ua1.csv";
    vector<string> variableList={"userID1", "weight1"};
    vector<vector<string>> ua1; 
    loadRawData(tableADD,variableList, ua1, '|');
    
    variableList={"userID1", "userID2"};
    tableADD="/media/ali/2TB/data/lastFM/SF4/A2/uf1.csv";
    vector<vector<string>> uf1; 
    loadRawData(tableADD,variableList, uf1, '|');
    
    variableList={"userID2", "userID3"};
    tableADD="/media/ali/2TB/data/lastFM/SF4/A2/uf2.csv";
    vector<vector<string>> uf2; 
    loadRawData(tableADD,variableList, uf2, '|');
    
    variableList={"userID3", "weight3"};
    tableADD="/media/ali/2TB/data/lastFM/SF4/A2/ua3.csv";
    vector<vector<string>> ua3; 
    loadRawData(tableADD,variableList, ua3, '|');
    cout<< "Loading the data finished"<<endl;
    
    double sum=0;
    for(int i=0;i<1;i++){
        cout<< "run:"<<i<<"------------------------------------------------------------------------"<<endl;
        auto start = std::chrono::system_clock::now();
        auto end = std::chrono::system_clock::now();
        std::chrono::duration<double> elapsed; 
//        do_seq_lastFM_A2(1,ua1,uf1,uf2,ua3);
        do_seq_lastFM_A2_select_star(1,ua1,uf1,uf2,ua3);
        end = std::chrono::system_clock::now();
        elapsed = end - start;
        sum+=elapsed.count();   
    }
    std::cout << "\nAverage elapsed wall time for sequential program with reduction: " << sum << "s"<<endl; 
    
//    
//    for(int numThreads=1;numThreads<2;numThreads++){
//       
//        double sum=0;
//        for(int i=0;i<50;i++){
//            cout<< "run:"<<i<<"------------------------------------------------------------------------"<<endl;
//            auto start = std::chrono::system_clock::now();
//            auto end = std::chrono::system_clock::now();
//            std::chrono::duration<double> elapsed; 
////            do_parallel_lastFM_A2(ua1,uf1,uf2,ua3,numThreads);
//            do_parallel_lastFM_A2_select_star(ua1,uf1,uf2,ua3,numThreads);
//
//            end = std::chrono::system_clock::now();
//            elapsed = end - start;
//            sum+=elapsed.count();
//        }
//        std::cout << "\n Average elapsed wall time for nested-parallel program with reduction and num of threads:"<<numThreads<< "  is: " << sum/50.0 << "s"<<endl;
//
//    }
       
    return 0;
}

