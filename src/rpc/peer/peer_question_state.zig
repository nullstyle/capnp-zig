const std = @import("std");

fn allocateQuestionId(
    comptime IdType: type,
    comptime QuestionType: type,
    questions: *std.AutoHashMap(IdType, QuestionType),
    next_question_id: *IdType,
    question: QuestionType,
) !IdType {
    const start_id = next_question_id.*;

    while (true) {
        const id = next_question_id.*;
        next_question_id.* +%= 1;

        if (!questions.contains(id)) {
            try questions.put(id, question);
            return id;
        }

        if (next_question_id.* == start_id) return error.QuestionIdExhausted;
    }
}

pub fn allocateQuestion(
    comptime QuestionType: type,
    questions: *std.AutoHashMap(u32, QuestionType),
    next_question_id: *u32,
    question: QuestionType,
) !u32 {
    return allocateQuestionId(u32, QuestionType, questions, next_question_id, question);
}

test "question allocation probes across wrap-around and then exhausts when ID space is full" {
    var questions = std.AutoHashMap(u8, u8).init(std.testing.allocator);
    defer questions.deinit();

    var id: u16 = 0;
    while (id <= std.math.maxInt(u8)) : (id += 1) {
        const question_id: u8 = @intCast(id);
        if (question_id == 253) continue;
        try questions.put(question_id, 1);
    }

    var next_question_id: u8 = 250;
    const allocated = try allocateQuestionId(u8, u8, &questions, &next_question_id, 9);
    try std.testing.expectEqual(@as(u8, 253), allocated);
    try std.testing.expectEqual(@as(u8, 254), next_question_id);
    try std.testing.expectEqual(@as(?u8, 9), questions.get(253));

    try std.testing.expectError(
        error.QuestionIdExhausted,
        allocateQuestionId(u8, u8, &questions, &next_question_id, 7),
    );
}
