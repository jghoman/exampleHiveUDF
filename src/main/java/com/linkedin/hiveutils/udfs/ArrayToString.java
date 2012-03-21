package com.linkedin.hiveutils.udfs;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Iterator;

public class ArrayToString extends GenericUDF {
  private ObjectInspector argOI;
  @Override
  public ObjectInspector initialize(ObjectInspector[] ois) throws UDFArgumentException {
    if(ois.length != 1) {
      throw new UDFArgumentException("Only one argument allowed");
    }
    argOI = ois[0];
    if(argOI.getCategory() != ObjectInspector.Category.LIST) {
      throw new UDFArgumentException("Only accepts lists");
    }

    return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
  }

  @Override
  public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
    if(deferredObjects[0].get() == null) return null;
    Object array = deferredObjects[0].get();

    ListObjectInspector loi = (ListObjectInspector)argOI;
    ArrayList al = (ArrayList)loi.getList(array);

    StringBuilder sb = new StringBuilder("[");

    Iterator iterator = al.iterator();
    while(iterator.hasNext()) {
      sb.append(iterator.next());
      if(iterator.hasNext())
        sb.append(",");
    }
    sb.append("]");
    return new Text(sb.toString());
  }

  @Override
  public String getDisplayString(String[] strings) {
    return "arrayToString("+strings[0] +")";
  }
}
