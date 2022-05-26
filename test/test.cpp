//#include "SkimmerHeader.h"
//std::string test(){
  //return FromDecimalToBinary(4481);

//}
// reading a text file
#include <iostream>
#include <fstream>
#include <string>
using namespace std;

void test () {
  std::string line;
  ifstream file ("/Users/valeriadamante/Desktop/Dottorato/lxplus/hhbbTauTauRes/Framework/config/pdg_name_type_charge.txt", ios::in );
  int k=0;
  while( std::getline(file,line) && k<1 )
    {
    std::stringstream ss(line);

    std::string part1, num1, part2 ,num2;
    std::getline(ss,num1,',');    std::cout<<stoi(num1) << "\t";
    std::getline(ss,part1,','); std::cout<<part1<<"\t";
    std::getline(ss,part2,','); std::cout<<part2<<"\t";
    std::getline(ss,num2,',');    std::cout<<stoi(num2) << "\t";
    std::cout<<"\n";
    k++;
  }

    file.close();
    return ;

  //return 0;
}
