#include "row_table.h"
#include <cstring>
#include <iostream> //myadd

namespace bytedance_db_project {
RowTable::RowTable() {}

RowTable::~RowTable() {
  if (rows_ != nullptr) {
    delete rows_;
    rows_ = nullptr;
  }
}

void RowTable::Load(BaseDataLoader *loader) {
  num_cols_ = loader->GetNumCols();
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                FIXED_FIELD_LEN * num_cols_);
  }
}

int32_t RowTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  if(row_id < 0 || row_id >= num_rows_ || col_id < 0 || col_id >= num_cols_)
  {
	  std::cout<<"RowTable::GetInField failed : the row_id or col_id is invalid or out of range.";
  	return -1;
  }
  int32_t  FieldData=*((int32_t*)(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_) + col_id * FIXED_FIELD_LEN));
  return FieldData;
}

void RowTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  if(row_id < 0 || col_id < 0 || col_id >= num_cols_)
  {
	std::cout<<"PutInFied failed : the row_id or col_id is invalid.";
  	return;
  }
  if(row_id >= num_rows_)
  {
  	char *temp=new char[FIXED_FIELD_LEN * (1+row_id) * num_cols_];
	std::memset(temp,0,sizeof(temp)/sizeof(field));
	std::memcpy(temp, rows_,
                FIXED_FIELD_LEN * num_cols_ * num_rows_);
	num_rows_=row_id+1;
  }
  *((int32_t*)(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_) + col_id * FIXED_FIELD_LEN))=field;

}

int64_t RowTable::ColumnSum() {
  // TODO: Implement this!
  int64_t res=0;
  for(int i = 0 ; i < num_rows_ ; i++)
  {
  	res+=*((int32_t*)(rows_ + i * (FIXED_FIELD_LEN * num_cols_)));
  }
  return res;
}

int64_t RowTable::PredicatedColumnSum(int32_t threshold1, int32_t threshold2) {
  // TODO: Implement this!
  int64_t res=0;
  for(int i = 0 ; i < num_rows_ ; i++)
  {
  	int32_t c1=*((int32_t*)(rows_ + i * (FIXED_FIELD_LEN * num_cols_) + FIXED_FIELD_LEN));
	int32_t c2=*((int32_t*)(rows_ + i * (FIXED_FIELD_LEN * num_cols_) + FIXED_FIELD_LEN*2));
	if(c1 > threshold1 && c2 < threshold2)
	{
		res+=*((int32_t*)(rows_ + i * (FIXED_FIELD_LEN * num_cols_)));
	}
  }
  return res;
}

int64_t RowTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t res=0;
  for(int i = 0 ; i < num_rows_ ; i++)
  {
        int32_t c0=*((int32_t*)(rows_ + i * (FIXED_FIELD_LEN * num_cols_)));
	if(c0 > threshold)
	{
		for(int j =0 ; j < num_cols_ ; j++)
		{
			res+=*((int32_t*)(rows_ + i * (FIXED_FIELD_LEN * num_cols_) + FIXED_FIELD_LEN*j));

		}	
	}
  }
  return res;
}

int64_t RowTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!i
  int row_count=0;
  for(int i = 0 ; i < num_rows_ ; i++ )
  {
  	int32_t c0=*((int32_t*)(rows_ + i * (FIXED_FIELD_LEN * num_cols_)));
	if(c0 < threshold)
	{
		int32_t *c2=(int32_t*)(rows_ + i * (FIXED_FIELD_LEN * num_cols_) + FIXED_FIELD_LEN*2);
		int32_t *c3=(int32_t*)(rows_ + i * (FIXED_FIELD_LEN * num_cols_) + FIXED_FIELD_LEN*3);
		*c3=*c2+*c3;
		row_count++;
	}
  }
  return row_count;
}
} // namespace bytedance_db_project
