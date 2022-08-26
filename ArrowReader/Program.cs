// See https://aka.ms/new-console-template for more information

using Apache.Arrow;
using Apache.Arrow.Ipc;

var arrowDir = new DirectoryInfo("/home/aron/DotnetProjects/ArrowTest/ArrowWriter");

var arrowFiles = arrowDir.GetFiles().Where(f => f.Extension == ".arrow");

Console.WriteLine($"Found {arrowFiles.Count()} arrow files.");

foreach (var arrowFile in arrowFiles)
{
    using (var stream = File.OpenRead(arrowFile.FullName))
    using (var reader = new ArrowFileReader(stream))
    {
        Console.WriteLine($"Processing {arrowFile}");
        var recordBatch = await reader.ReadNextRecordBatchAsync();
        Console.WriteLine("Read record batch with {0} column(s)", recordBatch.ColumnCount);
        for (int i = 0; i < recordBatch.ColumnCount; ++i)
        {
            try 
            {
                ReadColumn(recordBatch.Column(i));
            } catch (Exception e) { Console.WriteLine("didn't work");}
        }
        //return recordBatch;
    }
}

void ReadColumn(IArrowArray column)
{
    if (column.Data.DataType.TypeId == Apache.Arrow.Types.ArrowTypeId.Int64)
    {
        Console.WriteLine("int 64");
        var int64ArrowArray = (Int64Array) column;
        int64ArrowArray.ToList();
        
        //foreach (var l in int64ArrowArray.ToList()) Console.WriteLine(l);
    } else
    {
        Console.WriteLine("Unsupported type");
    }
}