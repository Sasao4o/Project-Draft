//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new ClockReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}



BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, LogManager *log_manager)
	    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
		      // We allocate a consecutive memory space for the buffer pool.
		         pages_ = new Page[pool_size_];
		           replacer_ = new ClockReplacer(pool_size);
		      
		             // Initially, every page is in the free list.
		               for (size_t i = 0; i < pool_size_; ++i) {
		                   free_list_.emplace_back(static_cast<int>(i));
		                     }
		                    }
BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  //Put All Changes Back To Disk 
    //We Have To Check If If Page Is Modified Or Not !
    Page *p=&pages_[page_table_[page_id]];
	     // u didn't even find the page (.end() act as NULL char)
	     if (page_table_.find(page_id)==page_table_.end()) {
		                  return false;
	     }
	    if (p->is_dirty_){
	     disk_manager_->WritePage(p->page_id_,p->GetData());
             p->is_dirty_=false;
	    } 
	   return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // We Have To Loop Over Buffer Pool
   for (size_t i = 0; i < pool_size_; i++) {
	FlushPgImp(pages_[i].page_id_);
   }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
        frame_id_t frame = -1;
	if (free_list_.empty()){
           if(!replacer_->Victim(&frame)){
		     return nullptr;
		     
		     } 
		     Page *oldpage =&pages_[frame];
		     FlushPgImp(oldpage->page_id_);
		    page_table_.erase(oldpage->page_id_);
          }else{
     frame=free_list_.front();
     free_list_.pop_front();
     
     }
		//pages_[frame].ResetMemory();
    	        *page_id=AllocatePage();
                 pages_[frame].page_id_=*page_id;
	        page_table_.insert({*page_id, frame});
		 // page_table_[*page_id]=frame;
	         pages_[frame].ResetMemory();
	         replacer_->Pin(frame);
	         pages[frame].pin_count_=1;
		 pages[frame].is_dirty_=false;
		 
  return &pages[frame];
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P dofind a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to Preturn nullp.
 
	  	//In Case i Find It In Memory No Need To Go To Disk
		if (page_table_.find(page_id) != page_table_.end()) {
				//In Case i Find It In Memory No Need To Go To Disk
					replacer_->Pin(page_table_[page_id]);
					            Page *p = &pages_[page_table_[page_id]];
					                        p->pin_count_++;
					                        	return &pages_[page_table_[page_id]];
					                        	}
					                        	frame_id_t myFrame;
					                        	if (free_list_.empty()) {
					                        		//Go To Victim
					                        		  if(replacer_->Victim(&myFrame)){
					                        		           FlushPgImp(pages_[myFrame].page_id_);
					                        		           }else{
					                        		           return nullptr;
					                        		           }
					                        		           } else {
					                        		           	//Bring Empty Slot
												myFrame = free_list_.front();
					                        		           	replacer_->Pin(myFrame);
					                        		           	    pages_[myFrame].pin_count_++;
					                        		           	       
													free_list_.erase(free_list_.begin());
					                        		           	        }
			                        		           	            Page *p = &pages_[myFrame];
					                        		           	                
					                        		           	                  disk_manager_->ReadPage(page_id, p->data_);
					                        		           	                       
					                        		           	                            replacer_->Pin(page_table_[page_id]);
					                        		           	                                p->pin_count_++;
					                        		           	                                    page_table_.erase(page_id);
					                        		           	                                        pages_[myFrame].ResetMemory();
					                        		           	                                            page_table_[page_id] = myFrame;
					                        		           	                                                return p;
				
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page tabfor the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  frame_id_t myFrame = page_table_[page_id];
  Page *trash =&pages_[myFrame];
   if(page_table_.find(page_id) == page_table_.end()) {
	   	//Oh No Need To Delete Its Logically Not There
			return true;
			}
			if (trash->GetPinCount()<1){
			free_list_.emplace_back(myFrame);
			pages_[page_table_[page_id]].ResetMemory();
		page_table_.erase(page_id);
		
		}
			return false;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) { return false; }

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
