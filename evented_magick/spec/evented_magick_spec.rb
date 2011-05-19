require File.expand_path('spec/spec_helper')

describe EventedMagick do
  describe :from_blob do
    it "can identify" do
      image = EventedMagick::Image.from_blob(File.read('spec/test.png'))
      image['width'].should == 100
    end
  end

  describe :new do
    it "can identify" do
      image = EventedMagick::Image.new('spec/test.png')
      image['width'].should == 100
    end
  end
end
