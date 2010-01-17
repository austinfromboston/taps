require 'active_record'
require 'active_support'
require 'stringio'
require 'uri'

module Taps
module Schema
  extend self

  def create_config(url)
    uri = URI.parse(url)
    adapter = uri.scheme
    adapter = 'postgresql' if adapter == 'postgres'
    adapter = 'sqlite3' if adapter == 'sqlite'
    config = {
      'adapter' => adapter,
      'database' => uri.path.blank? ? uri.host : uri.path.split('/')[1],
      'username' => uri.user,
      'password' => uri.password,
      'host' => uri.host,
    }
  end

  def connection(database_url)
    config = create_config(database_url)
    ActiveRecord::Base.establish_connection(config)
  end

  def dump(database_url)
    connection(database_url)

    stream = StringIO.new
    ActiveRecord::SchemaDumper.ignore_tables = []
    ActiveRecord::SchemaDumper.dump(ActiveRecord::Base.connection, stream)
    validate(stream).string
    #stream.string
  end

  def validate(stream)
    improved_string = stream.string.gsub( /:limit => 0,/, '' )
    improved_string.gsub!( /(^\s+t\.\w+\s+)"class"/, '\1"amp_class"' )
    StringIO.new validate_tables_without_primary_keys( improved_string )
  end

  def validate_tables_without_primary_keys( schema_string )
    #handle empty tables
    schema_string.gsub! /(\n\s*create_table "[-_A-z0-9]{0,255}", )(:force => true do \|t\|\n)(\s*end)/, 
      '\1:id=>false, \2    t.integer "id"' + "\n" + '\3'

    keyless_tables.each do |tbl|
      schema_string.gsub! /(\n\s*create_table "#{tbl}", )(:force => true do \|t\|\n)((\s*t\..+\n)+)(\s*end\n)/, 
        '\1:id=>false, \2    t.integer "id"' + "\n" + '\3\5'
    end
    schema_string
  end

  def forgettable_indexes
    [ "article_ft_idx", "title", "title_2", "index_articles_on_class" ]
  end

  def keyless_tables
    [ "phplist_subscribepage_data" ]
  end

  def dump_without_indexes(database_url)
    schema = dump(database_url)
    schema.split("\n").collect do |line|
      if line =~ /^\s+add_index/
        line = "##{line}"
      end
      line
    end.join("\n")
  end

  def indexes(database_url)
    schema = dump(database_url)
    schema.split("\n").collect do |line|
      line if line =~ /^\s+add_index/ and line !~ Regexp.new( ":name => \"(#{forgettable_indexes.join("|")})\"" )
    end.uniq.join("\n")
  end

  def load(database_url, schema)
    connection(database_url)
    eval(schema)
  end

  def load_indexes(database_url, indexes)
    connection(database_url)

    schema =<<EORUBY
ActiveRecord::Schema.define do
  #{indexes}
end
EORUBY
    eval(schema)
  end

  def reset_db_sequences(database_url)
    connection(database_url)

    if ActiveRecord::Base.connection.respond_to?(:reset_pk_sequence!)
      ActiveRecord::Base.connection.tables.each do |table|
        ActiveRecord::Base.connection.reset_pk_sequence!(table)
      end
    end
  end
end
end

module ActiveRecord
  class SchemaDumper
    private

    def header(stream)
      stream.puts "ActiveRecord::Schema.define do"
    end

    def tables(stream)
      @connection.tables.sort.each do |tbl|
        table(tbl, stream)
      end
    end
  end
end
