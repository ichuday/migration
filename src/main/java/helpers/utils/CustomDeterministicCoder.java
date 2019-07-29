package helpers.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;

public class CustomDeterministicCoder<T> extends Coder<T> {
	
	
	
	@Override
	public void encode(T value, OutputStream outStream) throws CoderException, IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public T decode(InputStream inStream) throws CoderException, IOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public List<? extends Coder<?>> getCoderArguments() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void verifyDeterministic() throws NonDeterministicException {
		// TODO Auto-generated method stub
		
	}

	
	
	
}
