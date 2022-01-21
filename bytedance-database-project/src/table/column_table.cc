#include "column_table.h"
#include <cstring>
#include <iostream>

namespace bytedance_db_project {
ColumnTable::ColumnTable() {}

ColumnTable::~ColumnTable() {
  if (columns_ != nullptr) {
    delete columns_;
    columns_ = nullptr;
  }
}

//
// columnTable, which stores data in column-major format.
// That is, data is laid out like
//   col 1 | col 2 | ... | col m.
//
void ColumnTable::Load(BaseDataLoader *loader) {
  num_cols_ = loader->GetNumCols();
  std::vector<char *> rows = loader->GetRows();
  num_rows_ = rows.size();
  columns_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];

  for (int32_t row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    for (int32_t col_id = 0; col_id < num_cols_; col_id++) {
      int32_t offset = FIXED_FIELD_LEN * ((col_id * num_rows_) + row_id);
      std::memcpy(columns_ + offset, cur_row + FIXED_FIELD_LEN * col_id,
                  FIXED_FIELD_LEN);
    }
  }
}

int32_t ColumnTable::GetIntField(int32_t row_id, int32_t col_id) {
  if(row_id < 0|| row_id >= num_rows_ || col_id < 0 || col_id >= num_cols_)
  {
	std::cout<<"ColumnTable::GetIntField failed : row_id or col_id is invalid. "<<std::endl;
  	return -1;
  }
  return *((int32_t*)(columns_ + FIXED_FIELD_LEN * (num_rows_ * col_id) +FIXED_FIELD_LEN * row_id));
}

void ColumnTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  if(row_id < 0|| col_id >= num_cols_ || col_id < 0)
  {
        std::cout<<"ColumnTable::GetIntField failed : row_id is out of range , or col_id is invalid. "<<std::endl;
        return;
  }
  if(row_id >= num_rows_)
  {
        char *temp=new char[FIXED_FIELD_LEN * num_rows_ * (1 + row_id )];
        std::memset(temp,0,sizeof(temp)/sizeof(field));
        std::memcpy(temp, columns_,
                FIXED_FIELD_LEN * num_cols_ * num_rows_);
        num_rows_=row_id + 1;
  }
  *((int32_t*)(columns_ + col_id * (FIXED_FIELD_LEN * num_rows_) + row_id * FIXED_FIELD_LEN))=field;
}

int64_t ColumnTable::ColumnSum() {
  int64_t res=0;
  for(int i = 0 ; i < num_rows_ ; i++)
  {
  	res+=*((int32_t*)(columns_ + i * FIXED_FIELD_LEN));
  }
  return res;
}

int64_t ColumnTable::PredicatedColumnSum(int32_t threshold1,
                                         int32_t threshold2) {
  // TODO: Implement this!
  int64_t res=0;
  for(int i = 0 ; i < num_rows_ ; i++)
  {
  	int32_t c1=*((int32_t*)(columns_ + i * FIXED_FIELD_LEN + 1 * (FIXED_FIELD_LEN * num_rows_)));
	int32_t c2=*((int32_t*)(columns_ + i * FIXED_FIELD_LEN + 2 * (FIXED_FIELD_LEN * num_rows_)));
	if(c1 > threshold1 && c2 < threshold2)
	{
		res+=*((int32_t*)(columns_ + i * FIXED_FIELD_LEN));
	}
  }
  return res;
}

int64_t ColumnTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t res=0;
  for(int i = 0 ;i < num_rows_ ; i++)
  {
  	int32_t c0 = *((int32_t*)(columns_ + i * FIXED_FIELD_LEN));
	if(c0 > threshold)
	{
		for(int j = 0 ;j < num_cols_ ; j++)
		{
			        int32_t c=*((int32_t*)(columns_ + i * FIXED_FIELD_LEN + j * (FIXED_FIELD_LEN * num_rows_)));
        			res+=c;
		}
	}
  }
  return res;
}

int64_t ColumnTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int row_count=0;
  for(int i = 0 ; i < num_rows_ ; i++ )
  {
  	int32_t c0 = *((int32_t*)(columns_ + i * FIXED_FIELD_LEN));
	if(c0 < threshold)
	{
		int32_t* c2=(int32_t*)(columns_ + i * FIXED_FIELD_LEN + 2 * (FIXED_FIELD_LEN * num_rows_));
		int32_t* c3=(int32_t*)(columns_ + i * FIXED_FIELD_LEN + 3 * (FIXED_FIELD_LEN * num_rows_));
		*c3=*c2+*c3;
		row_count++;
	}
  }
  return row_count;
}
} // namespace bytedance_db_project
