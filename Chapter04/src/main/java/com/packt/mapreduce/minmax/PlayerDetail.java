package com.packt.mapreduce.minmax;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PlayerDetail implements Writable {
    private Text playerName;
    private IntWritable score;
    private Text opposition;
    private LongWritable timestamps;
    private IntWritable ballsTaken;
    private IntWritable fours;
    private IntWritable six;

    public void readFields(DataInput dataInput) throws IOException {
        playerName.readFields(dataInput);
        score.readFields(dataInput);
        opposition.readFields(dataInput);
        timestamps.readFields(dataInput);
        ballsTaken.readFields(dataInput);
        fours.readFields(dataInput);
        six.readFields(dataInput);
    }

    public void write(DataOutput dataOutput) throws IOException {
        playerName.write(dataOutput);
        score.write(dataOutput);
        opposition.write(dataOutput);
        timestamps.write(dataOutput);
        ballsTaken.write(dataOutput);
        fours.write(dataOutput);
        playerName.write(dataOutput);
    }

    public Text getPlayerName() {
        return playerName;
    }

    public void setPlayerName(Text playerName) {
        this.playerName = playerName;
    }

    public IntWritable getScore() {
        return score;
    }

    public void setScore(IntWritable score) {
        this.score = score;
    }

    public Text getOpposition() {
        return opposition;
    }

    public void setOpposition(Text opposition) {
        this.opposition = opposition;
    }

    public LongWritable getTimestamps() {
        return timestamps;
    }

    public void setTimestamps(LongWritable timestamps) {
        this.timestamps = timestamps;
    }

    public IntWritable getBallsTaken() {
        return ballsTaken;
    }

    public void setBallsTaken(IntWritable ballsTaken) {
        this.ballsTaken = ballsTaken;
    }

    public IntWritable getFours() {
        return fours;
    }

    public void setFours(IntWritable fours) {
        this.fours = fours;
    }

    public IntWritable getSix() {
        return six;
    }

    public void setSix(IntWritable six) {
        this.six = six;
    }

    @Override
    public String toString() {
        return playerName +
                "\t" + score +
                "\t" + opposition +
                "\t" + timestamps +
                "\t" + ballsTaken +
                "\t" + fours +
                "\t" + six;
    }
}