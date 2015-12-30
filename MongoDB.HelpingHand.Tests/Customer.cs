using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace MongoDB.HelpingHand.Tests
{
    public class Customer
    {
        [BsonElement("_id")]
        [BsonId]
        public ObjectId Id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
    }
}
