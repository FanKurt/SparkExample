package com.imac.cassandra;
import java.io.Serializable;
import com.google.common.base.Objects;

 public class EMP implements Serializable {
        private Integer emp_id;
        private String emp_city;
        private String emp_name;
        private String emp_phone;
        private String emp_sal;

        public static EMP newInstance(Integer id, String city , String name , String emp_phone , String emp_sal) {
            EMP EMP = new EMP();
            EMP.setEmp_Id(id);
            EMP.setEmp_Name(name);
            EMP.setEmp_City(city);
            EMP.setEmp_Phone(null);
            EMP.setEmp_Sal(null);
            return EMP;
        }

        public Integer getEmp_Id() {
            return emp_id;
        }

        public void setEmp_Id(Integer id) {
            this.emp_id = id;
        }
        public String getEmp_Phone() {
            return emp_phone;
        }

        public void setEmp_Phone(String phone) {
            this.emp_phone = phone;
        }
        public String getEmp_Sal() {
            return emp_sal;
        }

        public void setEmp_Sal(String sal) {
            this.emp_sal = sal;
        }

        public String getEmp_Name() {
            return emp_name;
        }

        public void setEmp_Name(String name) {
            this.emp_name = name;
        }

        public String getEmp_City() {
            return emp_city;
        }
        public void setEmp_City(String city) {
            this.emp_city = city;
        }

        public String toString() {
            return Objects.toStringHelper(this)
                    .add("emp_id", emp_id)
                    .add("emp_city", emp_city)
                    .add("emp_name", emp_name)
                    .add("emp_phone", emp_phone)
                    .add("emp_sal", emp_sal)
                    .toString();
        }
    }