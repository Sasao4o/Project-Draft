//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// clock_replacer.cpp
//
// Identification: src/buffer/clock_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/clock_replacer.h"
using namespace std;
namespace bustub {

ClockReplacer::ClockReplacer(size_t num_pages) {
pointer=0;
size=num_pages;
clkreplacer=vector<bool>(num_pages,false);
available=vector<bool>(num_pages,false);
}

ClockReplacer::~ClockReplacer() = default;

bool ClockReplacer::Victim(frame_id_t *frame_id) { 
    int i=pointer;
    while(true){

     if(available[i]&&clkreplacer[i]){
       clkreplacer[i]=false;
       i=(i+1)%size;
       continue;
     }
     if(available[i]&&!clkreplacer[i]){
       available[i]=false;
       pointer=i;
       size--;
       /clkreplacer[pointer]=false;/
       break;
     }
     i=(i+1)%size;
     
    }
    
    
    
    
    
    
    
    return false; }

void ClockReplacer::Pin(frame_id_t frame_id) {
      //latch
      clkreplacer[frame_id]=false;
      available[frame_id]=false;

}

void ClockReplacer::Unpin(frame_id_t frame_id) {

    clkreplacer[frame_id]=true;
      available[frame_id]=true;
}

size_t ClockReplacer::Size() { return size; }

}  // namespace bustub