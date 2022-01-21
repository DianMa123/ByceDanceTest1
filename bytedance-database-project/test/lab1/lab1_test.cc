#include "data/csv_data_loader.h"
#include "table/column_table.h"
#include "table/custom_table.h"
#include "table/indexed_row_table.h"
#include "table/row_table.h"
#include "table/table.h"
#include "test_base.h"
#include <gtest/gtest.h>
#include <iostream>

using namespace bytedance_db_project;

class Lab1Test : public TestBase {};

TEST_F(Lab1Test, TestRowTable) {
  RowTable rt;
  rt.Load(loader);
  ASSERT_EQ(68, rt.ColumnSum());
}


TEST_F(Lab1Test, TestRowTable_GetIntField1) {
  RowTable rt;
  rt.Load(loader);
  ASSERT_EQ(3, rt.GetIntField(18,3));
}

TEST_F(Lab1Test, TestRowTable_PredicatedAllColumnsSum1) {
  RowTable rt;
  rt.Load(loader);
  ASSERT_EQ(166, rt.PredicatedAllColumnsSum(3));
}

TEST_F(Lab1Test, TestRowTable_PredicatedColumnSum1) {
  RowTable rt;
  rt.Load(loader);
  for(int i = 0 ; i < 20 ; i++)
  {
          if(i%2)
          {
                  ASSERT_EQ(2, rt.GetIntField(i,2));
                  rt.PutIntField(i,2,10);
          }
  }
   ASSERT_EQ(19, rt.PredicatedColumnSum(3,10));
}

TEST_F(Lab1Test, TestRowTable_PredicatedUpdate1) {
  RowTable rt;
  rt.Load(loader);
  ASSERT_EQ(3, rt.GetIntField(0,3));
  ASSERT_EQ(9, rt.PredicatedUpdate(3));
  ASSERT_EQ(5, rt.GetIntField(0,3));
}

TEST_F(Lab1Test, TestRowTable_PutIntField1) {
  RowTable rt;
  rt.Load(loader);
  ASSERT_EQ(3, rt.GetIntField(17,3));
  rt.PutIntField(17,3,9);
  ASSERT_EQ(9, rt.GetIntField(17,3));
}

TEST_F(Lab1Test, TestRowTable_UnionTest1) {
  RowTable rt;
  rt.Load(loader);
  //test GetIntField
  ASSERT_EQ(3, rt.GetIntField(18,3));
  
  //test PutIntField
  rt.PutIntField(18,3,9);
  ASSERT_EQ(9, rt.GetIntField(18,3));
  rt.PutIntField(18,3,9);
  ASSERT_EQ(9, rt.GetIntField(18,3));
  
  //test PredicatedAllColumnsSum
  ASSERT_EQ(172, rt.PredicatedAllColumnsSum(3));
  
  //test PredicatedColumnSum
  for(int i = 0 ; i < 20 ; i++)
  {
          if(i%2)
          {
                  ASSERT_EQ(2, rt.GetIntField(i,2));
                  rt.PutIntField(i,2,10);
          }
  }
  ASSERT_EQ(19, rt.PredicatedColumnSum(3,10));
  
  //test PredicatedUpdate
  ASSERT_EQ(3, rt.GetIntField(0,3));
  ASSERT_EQ(9, rt.PredicatedUpdate(3));
  ASSERT_EQ(5, rt.GetIntField(0,3));
  
}


/*column test*/

TEST_F(Lab1Test, TestColumnTable) {
  ColumnTable ct;
  ct.Load(loader);
  ASSERT_EQ(68, ct.ColumnSum());
}

TEST_F(Lab1Test, TestColumnTable_GetIntField1) {
  RowTable ct;
  ct.Load(loader);
  ASSERT_EQ(3, ct.GetIntField(18,3));
}


TEST_F(Lab1Test, TestColumnTable_PredicatedAllColumnsSum1) {
  ColumnTable ct;
  ct.Load(loader);
  ASSERT_EQ(166, ct.PredicatedAllColumnsSum(3));
}

TEST_F(Lab1Test, TestColumnTable_PredicatedColumnSum1) {
  ColumnTable ct;
  ct.Load(loader);
  for(int i = 0 ; i < 20 ; i++)
  {
          if(i%2)
          {
                  ASSERT_EQ(2, ct.GetIntField(i,2));
                  ct.PutIntField(i,2,10);
          }
  }
   ASSERT_EQ(19, ct.PredicatedColumnSum(3,10));
}

TEST_F(Lab1Test, TestColumnTable_PredicatedUpdate1) {
  ColumnTable ct;
  ct.Load(loader);
  ASSERT_EQ(3, ct.GetIntField(0,3));
  ASSERT_EQ(9, ct.PredicatedUpdate(3));
  ASSERT_EQ(5, ct.GetIntField(0,3));
}

TEST_F(Lab1Test, TestColumnTable_PutIntField1) {
  ColumnTable ct;
  ct.Load(loader);
  ASSERT_EQ(3, ct.GetIntField(17,3));
  ct.PutIntField(17,3,9);
  ASSERT_EQ(9, ct.GetIntField(17,3));
}

TEST_F(Lab1Test, TestRowTable_UnionTest2) {
  RowTable ct;
  ct.Load(loader);
  //test GetIntField
  ASSERT_EQ(3, ct.GetIntField(18,3));
  
  //test PutIntField
  ct.PutIntField(18,3,9);
  ASSERT_EQ(9, ct.GetIntField(18,3));
  ct.PutIntField(18,3,9);
  ASSERT_EQ(9, ct.GetIntField(18,3));
  
  //test PredicatedAllColumnsSum
  ASSERT_EQ(172, ct.PredicatedAllColumnsSum(3));
  
  //test PredicatedColumnSum
  for(int i = 0 ; i < 20 ; i++)
  {
          if(i%2)
          {
                  ASSERT_EQ(2, ct.GetIntField(i,2));
                  ct.PutIntField(i,2,10);
          }
  }
  ASSERT_EQ(19, ct.PredicatedColumnSum(3,10));
  
  //test PredicatedUpdate
  ASSERT_EQ(3, ct.GetIntField(0,3));
  ASSERT_EQ(9, ct.PredicatedUpdate(3));
  ASSERT_EQ(5, ct.GetIntField(0,3));
  
}
