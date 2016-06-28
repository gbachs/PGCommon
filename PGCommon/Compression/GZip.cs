using System.IO;
using System.IO.Compression;

namespace PGCommon.Compression
{
    public static class GZip
    {
        public static byte[] Compress(byte[] bytes)
        {
            using (var stream = new MemoryStream())
            using (var zipStream = new GZipStream(stream, CompressionMode.Compress, true))
            {
                zipStream.Write(bytes, 0, bytes.Length);
                return stream.ToArray();
            }
        }

        public static byte[] Decompress(byte[] bytes)
        {
            using (var stream = new MemoryStream())
            using (var zipStream = new GZipStream(new MemoryStream(bytes), CompressionMode.Decompress, true))
            {
                var buffer = new byte[4096];
                while (true)
                {
                    var size = zipStream.Read(buffer, 0, buffer.Length);
                    if (size > 0)
                    {
                        stream.Write(buffer, 0, size);
                    }
                    else
                    {
                        break;
                    }
                }
                return stream.ToArray();
            }
        }
    }
}
